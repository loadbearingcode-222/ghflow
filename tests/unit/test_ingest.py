"""One hour through ingest, offline: a dict stands in for S3 and a list for the audit table."""

from __future__ import annotations

import gzip
import io
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from factories import HOUR, line, push
from ghflow import audit, bronze
from ghflow.commands import ingest
from ghflow.exitcodes import ExitCode
from ghflow.storage import sha256


class FakeStore:
    def __init__(self, fail_on: str | None = None) -> None:
        self.objects: dict[str, bytes] = {}
        self.fail_on = fail_on

    def put(self, path: str, data: bytes) -> str:
        if self.fail_on and self.fail_on in path:
            raise RuntimeError("disk on fire")
        self.objects[path] = data
        return sha256(data)


@pytest.fixture
def recorded(monkeypatch: pytest.MonkeyPatch) -> list[audit.Attempt]:
    rows: list[audit.Attempt] = []

    def record(conn: Any, attempt: audit.Attempt) -> int:
        rows.append(attempt)
        return len(rows)

    monkeypatch.setattr(audit, "record", record)
    return rows


def hour_file(tmp_path: Path, *lines: bytes) -> Path:
    path = tmp_path / f"{HOUR.key}.json.gz"
    path.write_bytes(gzip.compress(b"".join(lines)))
    return path


def good_hour(tmp_path: Path) -> Path:
    bad = b'{"id": "1", "type": "PushEvent"\n'  # unparseable: rejected into the sidecar
    return hour_file(tmp_path, line(push(40_000_000_001)), line(push(40_000_000_002)), bad)


def test_an_accepted_hour_writes_bronze_and_sidecar_and_one_audit_row(
    tmp_path: Path, recorded: list[audit.Attempt]
) -> None:
    store = FakeStore()
    code = ingest.run(HOUR, good_hour(tmp_path), store, conn=None)  # type: ignore[arg-type]
    assert code == ExitCode.PASSED
    [attempt] = recorded
    assert (attempt.outcome, attempt.rows_in, attempt.rows_accepted, attempt.rows_rejected) == (
        "accepted",
        3,
        2,
        1,
    )
    events = pq.read_table(io.BytesIO(store.objects[bronze.bronze_path(HOUR)]))
    sidecar = pq.read_table(io.BytesIO(store.objects[bronze.sidecar_path(HOUR)]))
    assert events.num_rows + sidecar.num_rows == attempt.rows_in
    assert sidecar.to_pylist()[0]["rule"] == "unparseable_json"
    assert bronze.raw_path(HOUR) not in store.objects, "a planted file never lands in raw"
    assert attempt.bronze_sha256 == sha256(store.objects[bronze.bronze_path(HOUR)])


def test_a_rerun_rewrites_identical_bytes_and_appends_a_row(
    tmp_path: Path, recorded: list[audit.Attempt]
) -> None:
    store = FakeStore()
    source = good_hour(tmp_path)
    ingest.run(HOUR, source, store, conn=None)  # type: ignore[arg-type]
    first = dict(store.objects)
    ingest.run(HOUR, source, store, conn=None)  # type: ignore[arg-type]
    assert store.objects == first, "same paths, same bytes: no second partition"
    assert [a.bronze_sha256 for a in recorded] == [recorded[0].bronze_sha256] * 2


def test_a_structure_change_refuses_the_hour_and_writes_no_bronze(
    tmp_path: Path, recorded: list[audit.Attempt]
) -> None:
    planted = push(40_000_000_003)
    planted["surprise"] = True  # an unknown top-level key
    store = FakeStore()
    source = hour_file(tmp_path, line(push(40_000_000_001)), line(planted))
    code = ingest.run(HOUR, source, store, conn=None)  # type: ignore[arg-type]
    assert code == ExitCode.REFUSED and int(code) == 1
    assert store.objects == {}
    [attempt] = recorded
    assert (attempt.outcome, attempt.refusal_code) == ("refused", "STRUCTURE-UNKNOWN-KEY")


def test_a_crash_is_recorded_then_raised(tmp_path: Path, recorded: list[audit.Attempt]) -> None:
    store = FakeStore(fail_on="gharchive_rejected")
    with pytest.raises(RuntimeError):
        ingest.run(HOUR, good_hour(tmp_path), store, conn=None)  # type: ignore[arg-type]
    [attempt] = recorded
    assert attempt.outcome == "crashed" and attempt.duration_ms is not None


def test_a_failed_fetch_is_recorded_and_exits_2(
    monkeypatch: pytest.MonkeyPatch, recorded: list[audit.Attempt]
) -> None:
    def refuse(hour: Any) -> Any:
        raise OSError("connection reset")

    monkeypatch.setattr(ingest, "fetch_hour", refuse)
    assert ingest.run(HOUR, None, FakeStore(), conn=None) == ExitCode.COULD_NOT_BUILD  # type: ignore[arg-type]
    assert recorded[0].outcome == "failed" and "connection reset" in (recorded[0].detail or "")


def test_the_attempt_printed_is_json(
    tmp_path: Path, recorded: list[audit.Attempt], capsys: pytest.CaptureFixture[str]
) -> None:
    ingest.run(HOUR, good_hour(tmp_path), FakeStore(), conn=None)  # type: ignore[arg-type]
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["outcome"] == "accepted"

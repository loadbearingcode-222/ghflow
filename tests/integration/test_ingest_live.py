"""Ingest against the running platform: real S3 (SeaweedFS) and the real audit table.

Planted, synthetic hours only, at hours far outside the backfill window (2020), so a real
hour's bronze or verdict is never touched. Audit rows are append-only and stay, marked with
their `file:` source; the bronze objects a test writes are deleted afterwards.
"""

from __future__ import annotations

import gzip
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from test_platform import env

from ghflow import audit, bronze
from ghflow.commands import ingest
from ghflow.exitcodes import ExitCode
from ghflow.hours import Hour
from ghflow.storage import S3Config, Store, sha256

pytestmark = pytest.mark.integration


@pytest.fixture
def store() -> Store:
    secrets = env()
    return Store(
        S3Config("http://127.0.0.1:39000", secrets["S3_ACCESS_KEY"], secrets["S3_SECRET_KEY"])
    )


@pytest.fixture
def conn() -> Iterator[Any]:
    dsn = f"postgresql://ghflow:{env()['GHFLOW_OPS_PASSWORD']}@127.0.0.1:35432/ghflow_ops"
    with audit.connect(dsn) as connection:
        yield connection


@pytest.fixture
def hour(store: Store) -> Iterator[Hour]:
    """A fresh 2020 hour per test (the minute of a uuid keeps parallel runs apart)."""
    picked = Hour.parse(f"2020-01-{1 + uuid.uuid4().int % 28:02d}-{uuid.uuid4().int % 24}")
    yield picked
    for path in (bronze.bronze_path(picked), bronze.sidecar_path(picked)):
        if store.exists(path):
            store.fs.delete_file(path)


def planted(tmp_path: Path, hour: Hour, extra_key: bool = False) -> Path:
    created = hour.start.strftime("%Y-%m-%dT%H:07:00Z")
    lines = []
    for n in range(3):
        surprise = ', "surprise": true' if extra_key and n == 2 else ""
        lines.append(
            f'{{"id": "{40_000_000_000 + n}", "type": "WatchEvent", "actor": {{"id": 1, '
            f'"login": "synthetic-dev"}}, "repo": {{"id": 2, "name": "synthetic-org/repo"}}, '
            f'"payload": {{"action": "started"}}, "public": true, "created_at": "{created}"'
            f"{surprise}}}\n"
        )
    path = tmp_path / f"{hour.key}.json.gz"
    path.write_bytes(gzip.compress("".join(lines).encode()))
    return path


def test_an_hour_lands_and_a_rerun_rewrites_identical_bytes(
    tmp_path: Path, store: Store, conn: Any, hour: Hour
) -> None:
    source = planted(tmp_path, hour)
    assert ingest.run(hour, source, store, conn) == ExitCode.PASSED
    first = sha256(store.get(bronze.bronze_path(hour)))
    assert ingest.run(hour, source, store, conn) == ExitCode.PASSED
    assert sha256(store.get(bronze.bronze_path(hour))) == first
    assert store.files(
        f"bronze/gharchive/date={hour.day.isoformat()}/hour={hour.start.hour:02d}"
    ) == [bronze.bronze_path(hour)], "a re-run replaces the file, never adds a second one"
    rows = audit.attempts(conn, hour.key)[-2:]
    assert [r["outcome"] for r in rows] == ["accepted", "accepted"]
    assert {r["bronze_sha256"] for r in rows} == {first}
    assert rows[-1]["rows_in"] == rows[-1]["rows_accepted"] + rows[-1]["rows_rejected"] == 3


def test_a_planted_structure_change_is_refused_with_no_bronze(
    tmp_path: Path, store: Store, conn: Any, hour: Hour
) -> None:
    assert ingest.run(hour, planted(tmp_path, hour, extra_key=True), store, conn) == 1
    assert not store.exists(bronze.bronze_path(hour))
    last = audit.attempts(conn, hour.key)[-1]
    assert (last["outcome"], last["refusal_code"]) == ("refused", "STRUCTURE-UNKNOWN-KEY")

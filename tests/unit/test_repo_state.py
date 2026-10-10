"""The repo-state processor (M3-S2) against fake Kafka clients: net changes, transactions, restart.

Synthetic repo ids and names only. The live drills (kill and restart, diff against dim_repo, a
planted stale record) run in the stack through `python -m ghflow.commands.repo_state`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ghflow.commands import repo_state as cli
from ghflow.stream import repo_state
from ghflow.stream.repo_state import PartitionHour, Processor, net_changes
from ghflow.stream.state_store import StateStore, Stored, micros


def at(hour: int, second: int = 0) -> datetime:
    return datetime(2026, 8, 3, hour, 0, second, tzinfo=UTC)


def event(event_id: int, created: datetime, repo: int, name: str | None) -> dict[str, Any]:
    hour = f"2026-08-03-{created.hour}"
    return {"kind": "EVENT", "hour": hour, "id": event_id, "created_at": created,
            "repo_id": repo, "repo_name": name}  # fmt: skip


def marker(hour: int) -> dict[str, Any]:
    return {"kind": "END_OF_HOUR", "hour": f"2026-08-03-{hour}", "partition": 0}


class FakeProducer:
    """Records produced inside a transaction become visible only when it commits."""

    def __init__(self, crash_before_commit: bool = False) -> None:
        self.committed: list[tuple[bytes, dict[str, Any]]] = []
        self.offsets: list[int] = []
        self.calls: list[str] = []
        self.open: list[tuple[bytes, dict[str, Any]]] = []
        self.crash_before_commit = crash_before_commit

    def begin_transaction(self) -> None:
        self.calls.append("begin")
        self.open = []

    def produce(self, topic: str, key: bytes, value: bytes, partition: int) -> None:
        assert topic == repo_state.STATE_TOPIC
        assert partition == 0, "a repo's state goes to its events' partition"
        self.calls.append("produce")
        self.open.append((key, repo_state.decode(value)))

    def send_offsets_to_transaction(self, offsets: list[Any], _meta: Any) -> None:
        self.calls.append("offsets")
        self.pending_offset = offsets[0].offset

    def commit_transaction(self) -> None:
        if self.crash_before_commit:
            raise RuntimeError("killed before the commit")
        self.calls.append("commit")
        self.committed += self.open
        self.offsets.append(self.pending_offset)


class FakeConsumer:
    """The changelog's end is the count of committed records plus one marker per commit."""

    def __init__(self, producer: FakeProducer) -> None:
        self.producer = producer

    def consumer_group_metadata(self) -> object:
        return object()

    def get_watermark_offsets(self, _tp: Any, timeout: float, cached: bool) -> tuple[int, int]:
        return 0, len(self.producer.committed) + len(self.producer.offsets)


def processor(tmp_path: Path, producer: FakeProducer, name: str = "s.sqlite") -> Processor:
    serialize = repo_state.Serializer(7)
    store = StateStore(tmp_path / name, fresh=True)
    return Processor(FakeConsumer(producer), producer, store, serialize)  # type: ignore[arg-type]


def feed(proc: Processor, records: list[dict[str, Any]], first_offset: int = 0) -> list[Any]:
    return [c for n, r in enumerate(records) if (c := proc.take(0, first_offset + n, r))]


HOUR_1 = [event(1, at(1, 5), 10, "o/a"), event(2, at(1, 9), 11, "o/x"), event(3, at(1, 7), 10, "o/b"),
          marker(1)]  # fmt: skip
HOUR_2 = [event(4, at(2, 1), 10, "o/b"), event(5, at(2, 2), 11, "o/y"), event(6, at(2, 3), 11, "o/x"),
          event(7, at(2, 3), 12, None), marker(2)]  # fmt: skip
HOUR_3 = [event(8, at(3, 1), 10, "o/c"), event(9, at(3, 1), 12, "o/n"), marker(3)]


def test_the_hour_final_name_follows_created_at_then_event_id_not_arrival() -> None:
    buf = PartitionHour("2026-08-03-1")
    for record in [event(2, at(1, 9), 10, "o/late"), event(1, at(1, 9), 10, "o/tie-lower-id"),
                   event(3, at(1, 2), 10, "o/early")]:  # fmt: skip
        buf.add(record)
    assert buf.latest[10][2] == "o/late"


def test_net_changes_new_renamed_unchanged_and_older() -> None:
    buf = PartitionHour("2026-08-03-5")
    for record in [event(1, at(5), 1, "o/new"), event(2, at(5), 2, "o/renamed"),
                   event(3, at(5), 3, "o/same"), event(4, at(5), 4, "o/older-replay")]:  # fmt: skip
        buf.add(record)
    late = micros(at(6))
    stored = {2: Stored("o/before", 0, 0), 3: Stored("o/same", 0, 0), 4: Stored("o/now", late, 9)}
    assert [c["repo_id"] for c in net_changes(buf, stored)] == [1, 2]


def test_a_flip_back_within_an_hour_writes_nothing() -> None:
    buf = PartitionHour("2026-08-03-5")
    for record in [event(1, at(5, 1), 1, "o/b"), event(2, at(5, 2), 1, "o/a")]:
        buf.add(record)
    assert net_changes(buf, {1: Stored("o/a", 0, 0)}) == []


def test_changes_and_the_offset_commit_in_one_transaction(tmp_path: Path) -> None:
    producer = FakeProducer()
    closed = feed(processor(tmp_path, producer), HOUR_1)
    assert producer.calls == ["begin", "produce", "produce", "offsets", "commit"]
    assert producer.offsets == [4]  # past the marker at offset 3
    assert [(r["repo_id"], r["repo_name"]) for _, r in producer.committed] == [
        (10, "o/b"),
        (11, "o/x"),
    ]
    assert closed == [
        {"hour": "2026-08-03-1", "partition": 0, "events": 3, "repos": 2, "changes": 2}
    ]


def test_a_marker_with_no_changes_still_moves_the_offset(tmp_path: Path) -> None:
    producer = FakeProducer()
    proc = processor(tmp_path, producer)
    feed(proc, HOUR_1)
    feed(proc, [event(4, at(2), 10, "o/b"), marker(2)], first_offset=4)
    assert producer.offsets == [4, 6]
    assert len(producer.committed) == 2


def test_a_restart_rebuilds_from_the_changelog_and_emits_nothing_spurious(tmp_path: Path) -> None:
    """Uninterrupted run vs: two hours, killed inside hour 3's transaction, store rebuilt from the
    committed changelog, hour 3 re-read from the committed offset. Same changelog, record for record."""
    straight = FakeProducer()
    feed(processor(tmp_path, straight, "a.sqlite"), HOUR_1 + HOUR_2 + HOUR_3)

    first = FakeProducer()
    proc = processor(tmp_path, first, "b.sqlite")
    feed(proc, HOUR_1 + HOUR_2)
    first.crash_before_commit = True
    with pytest.raises(RuntimeError, match="killed"):
        feed(proc, HOUR_3, first_offset=9)
    resumed_at = first.offsets[-1]
    assert resumed_at == 9

    second = FakeProducer()
    restarted = processor(tmp_path, second, "c.sqlite")
    restarted.store.put(r for _, r in first.committed)  # what restore() reads back
    feed(restarted, HOUR_3, first_offset=resumed_at)
    assert first.committed + second.committed == straight.committed
    assert [r["repo_name"] for _, r in second.committed] == ["o/c", "o/n"]


def test_hours_never_mix_on_a_partition(tmp_path: Path) -> None:
    proc = processor(tmp_path, FakeProducer())
    proc.take(0, 0, event(1, at(1), 10, "o/a"))
    with pytest.raises(RuntimeError, match="refusing to mix hours"):
        proc.take(0, 1, event(2, at(2), 10, "o/a"))


def test_the_store_round_trips(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "s.sqlite", fresh=True)
    store.put([{"repo_id": 5, "repo_name": "o/q", "as_of": at(4), "event_id": 3, "hour": "x"}])
    assert store.lookup([5, 6]) == {5: Stored("o/q", micros(at(4)), 3)}
    assert store.count() == 1


def test_the_committed_avsc_is_the_schema() -> None:
    assert cli.schema_file(check_only=True) == 0


def test_the_planted_stale_record_is_caught_only_when_it_alone_is_red() -> None:
    planted = {"repo_id": 42}
    alone = {"counts": {"equal": {}, "ahead": {}, "stale": {"named": [42]}}, "planted": planted}
    noisy = {
        "counts": {"equal": {}, "stale": {"named": [42]}, "missing_in_topic": {}},
        "planted": planted,
    }
    wrong = {"counts": {"equal": {}, "stale": {"named": [41]}}, "planted": planted}
    assert cli._caught(alone) and not cli._caught(noisy) and not cli._caught(wrong)


def test_the_store_and_its_position_survive_a_reopen(tmp_path: Path) -> None:
    """R1 (M3-S3): what a restarted processor finds on its volume."""
    producer = FakeProducer()
    proc = processor(tmp_path, producer)
    feed(proc, HOUR_1)
    proc.store.db.close()  # as a killed process leaves it
    reopened = StateStore(tmp_path / "s.sqlite")
    assert reopened.count() == 2
    assert reopened.positions() == {0: 3}  # 2 records + 1 commit marker: the next offset
    assert reopened.lookup([10])[10].name == "o/b"


def test_a_fresh_store_forgets_everything(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "s.sqlite", fresh=True)
    store.put([{"repo_id": 5, "repo_name": "o/q", "as_of": at(4), "event_id": 3}], {0: 9})
    store.db.close()
    again = StateStore(tmp_path / "s.sqlite", fresh=True)
    assert again.count() == 0 and again.positions() == {}


def test_an_unreadable_store_raises_the_error_that_triggers_a_full_rebuild(tmp_path: Path) -> None:
    import sqlite3

    (tmp_path / "s.sqlite").write_bytes(b"not a database, " * 100)
    with pytest.raises(sqlite3.DatabaseError):
        StateStore(tmp_path / "s.sqlite")

"""The lander and compaction, offline: fakes stand in for Kafka, S3 and Postgres."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pytest

from ghflow import bronze
from ghflow.storage import sha256
from ghflow.stream import compact, lander

HOUR = "2026-08-03-0"


class FakeStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put(self, path: str, data: bytes) -> str:
        self.objects[path] = data
        return sha256(data)

    def get(self, path: str) -> bytes:
        return self.objects[path]


class FakeConsumer:
    def __init__(self) -> None:
        self.commits: list[tuple[int, int]] = []

    def commit(self, offsets: list[Any], asynchronous: bool) -> None:
        self.commits += [(tp.partition, tp.offset) for tp in offsets]


class FakeConn:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []

    def execute(self, sql: bytes, params: tuple[Any, ...] = ()) -> None:
        self.rows.append(params)


def event(event_id: int, hour: str = HOUR, **overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = dict.fromkeys(bronze.SCHEMA.names)
    row |= {
        "id": event_id,
        "type": "PushEvent",
        "repo_id": 2,
        "created_at": datetime(2026, 8, 3, tzinfo=UTC),
    }
    return {"kind": "EVENT", "hour": hour, **row, **overrides}


def marker(partition: int, hour: str = HOUR) -> dict[str, Any]:
    return {"kind": "END_OF_HOUR", "hour": hour, "partition": partition, "rows_in_hour": 2}


def landing() -> tuple[lander.Lander, FakeConsumer, FakeStore, FakeConn]:
    consumer, store, conn = FakeConsumer(), FakeStore(), FakeConn()
    return lander.Lander(consumer, store, conn), consumer, store, conn  # type: ignore[arg-type]


def test_a_partition_lands_at_its_marker_named_by_its_offset_range_and_commits_after() -> None:
    land, consumer, store, conn = landing()
    assert land.take(3, 10, event(1)) is None
    assert land.take(3, 11, event(2)) is None
    assert consumer.commits == [] and store.objects == {}, "nothing is committed before the marker"
    landed = land.take(3, 12, marker(3))
    assert landed == {"hour": HOUR, "partition": 3, "first": 10, "last": 12, "events": 2}
    assert list(store.objects) == [lander.landed_path(3, 10, 12)]
    assert lander.landed_path(3, 10, 12).endswith(
        "partition=3/00000000000000000010-00000000000000000012.parquet"
    )
    assert consumer.commits == [(3, 13)], "commit the marker + 1, after the write"
    assert conn.rows[-1][:5] == (HOUR, 3, 10, 12, 2)


def test_relanding_the_same_range_writes_the_same_object() -> None:
    first, _, store_a, _ = landing()
    second, _, store_b, _ = landing()
    for land in (first, second):
        land.take(0, 5, event(1))
        land.take(0, 6, marker(0))
    assert store_a.objects == store_b.objects


def test_a_buffer_never_mixes_hours() -> None:
    land, *_ = landing()
    land.take(1, 0, event(1, hour="2026-08-03-0"))
    with pytest.raises(RuntimeError, match="refusing to mix hours"):
        land.take(1, 1, event(2, hour="2026-08-03-1"))


def landed_table(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(
        [{k: r.get(k) for k in lander.LANDED_SCHEMA.names} for r in rows],
        schema=lander.LANDED_SCHEMA,
    )


def test_compaction_keeps_each_event_once_and_drops_markers() -> None:
    a = landed_table(
        [{"_offset": 0, **event(2)}, {"_offset": 1, **event(1)}, {"_offset": 2, **marker(0)}]
    )
    b = landed_table([{"_offset": 0, **event(2)}, {"_offset": 1, **marker(1)}])  # a duplicate of 2
    result = compact.compact([a, b])
    assert result.table.column("id").to_pylist() == [1, 2]
    assert (result.input_events, result.duplicates) == (3, 1)
    assert result.table.schema == bronze.SCHEMA
    assert compact.content_equal([a, b], result.table)


def test_a_duplicate_with_different_values_is_refused() -> None:
    a = landed_table([{"_offset": 0, **event(1)}])
    b = landed_table([{"_offset": 0, **event(1, repo_id=99)}])
    with pytest.raises(compact.CompactionRefusal):
        compact.compact([a, b])


def test_content_equal_catches_a_missing_event() -> None:
    a = landed_table([{"_offset": 0, **event(1)}, {"_offset": 1, **event(2)}])
    result = compact.compact([a])
    assert not compact.content_equal([a], result.table.slice(0, 1))

"""Compact one closed hour's landed files into one file of distinct events. Pure: tables in, out.

The landed files of an hour hold its events once per publication (a replayer killed mid-hour
publishes part of it twice) plus its six END_OF_HOUR markers. Compaction keeps each event id once
(its first publication, lowest offset), drops the markers, sorts by id, and checks the result is
content-equal to its inputs: every input event id is present, and every duplicate of an id carries
exactly the same values. A duplicate that disagrees is refused, never silently resolved.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ghflow import bronze


class CompactionRefusal(ValueError):
    pass


@dataclass(frozen=True)
class Compacted:
    table: pa.Table
    input_events: int
    duplicates: int


def compact(landed: list[pa.Table]) -> Compacted:
    """Distinct events of one hour, sorted by id, in bronze's schema."""
    rows = pa.concat_tables(landed)
    events = rows.filter(pc.equal(rows["kind"], "EVENT")).sort_by("_offset")
    seen: dict[int, tuple[object, ...]] = {}
    keep = []
    for index, row in enumerate(events.select(bronze.SCHEMA.names).to_pylist()):
        values = tuple(row.values())
        if row["id"] in seen:
            if seen[row["id"]] != values:
                raise CompactionRefusal(
                    f"event {row['id']} was published twice with different values"
                )
            continue
        seen[row["id"]] = values
        keep.append(index)
    distinct = events.take(keep).select(bronze.SCHEMA.names).cast(bronze.SCHEMA).sort_by("id")
    return Compacted(distinct, input_events=events.num_rows, duplicates=events.num_rows - len(keep))


def content_equal(landed: list[pa.Table], output: pa.Table) -> bool:
    """Every input event id, and only those, with the same values: the compaction's own proof."""
    events = pa.concat_tables(landed)
    events = events.filter(pc.equal(events["kind"], "EVENT")).select(bronze.SCHEMA.names)
    wanted = {r["id"]: tuple(r.values()) for r in events.to_pylist()}
    got = {r["id"]: tuple(r.values()) for r in output.to_pylist()}
    return wanted == got and len(got) == output.num_rows


def encode(table: pa.Table, hour_key: str) -> bytes:
    """Deterministic, like bronze: the same events always give the same bytes."""
    sink = pa.BufferOutputStream()
    pq.write_table(
        table.replace_schema_metadata({"ghflow.hour": hour_key}), sink, compression="zstd"
    )
    return bytes(sink.getvalue().to_pybytes())


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

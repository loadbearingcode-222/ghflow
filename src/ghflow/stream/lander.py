"""Land gharchive.events in S3, one file per (partition, hour), committing only after the write.

A partition's records are buffered until that hour's END_OF_HOUR marker arrives on it. The file
then covers exactly [first buffered offset, marker offset] and is named by that range; only after
the PUT succeeds is the partition's offset committed (marker + 1) and the (hour, partition) row
written. A lander killed anywhere restarts from its last commit, reaches the same marker, and
rewrites the same file with the same content: the landed set covers every consumed offset exactly
once (ADR-0006).

The replayer publishes hours in order, so a buffer only ever holds one hour. If it ever held two,
the lander refuses (raises) rather than file records under the wrong hour.
"""

from __future__ import annotations

import io
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, TopicPartition

from ghflow import bronze
from ghflow.storage import Store
from ghflow.stream import clients

TOPIC = clients.TOPIC
GROUP = clients.LANDER_GROUP
BUCKET = "stream"

HOURS_SQL = b"""
create table if not exists stream_hours (
    hour          text        not null,
    partition     int         not null,
    first_offset  bigint      not null,
    last_offset   bigint      not null,
    events        bigint      not null,
    path          text        not null,
    landed_at     timestamptz not null default now(),
    primary key (hour, partition)
);
create table if not exists stream_compacted (
    hour           text        primary key,
    input_files    int         not null,
    input_events   bigint      not null,
    duplicates     bigint      not null,
    output_rows    bigint      not null,
    output_sha256  text        not null,
    compacted_at   timestamptz not null default now()
);
"""

LANDED_SCHEMA = pa.schema(
    [
        pa.field("_offset", pa.int64()),
        pa.field("kind", pa.string()),
        pa.field("hour", pa.string()),
        *bronze.SCHEMA,
    ]
)


def landed_path(partition: int, first: int, last: int) -> str:
    """Named by its offset range: re-landing the same range writes the same object."""
    return f"{BUCKET}/landed/topic={TOPIC}/partition={partition}/{first:020d}-{last:020d}.parquet"


def compact_path(hour_key: str) -> str:
    day, _, hh = hour_key.rpartition("-")
    return f"{BUCKET}/compact/date={day}/hour={int(hh):02d}/events.parquet"


@dataclass
class Buffer:
    hour: str | None = None
    rows: list[dict[str, Any]] = field(default_factory=list)


class Lander:
    def __init__(
        self, consumer: Consumer, store: Store, conn: psycopg.Connection[Any], topic: str = TOPIC
    ) -> None:
        self.consumer, self.store, self.conn, self.topic = consumer, store, conn, topic
        self.buffers: dict[int, Buffer] = defaultdict(Buffer)

    def take(self, partition: int, offset: int, record: dict[str, Any]) -> dict[str, Any] | None:
        """Buffer one record; at its hour's marker, land the partition. Returns what was landed."""
        buf = self.buffers[partition]
        if buf.hour is not None and record["hour"] != buf.hour:
            raise RuntimeError(
                f"partition {partition}: {record['hour']} arrived before {buf.hour}'s marker; refusing to mix hours"
            )
        buf.hour = record["hour"]
        buf.rows.append({"_offset": offset, **record})
        return self.land(partition) if record["kind"] == "END_OF_HOUR" else None

    def land(self, partition: int) -> dict[str, Any]:
        buf = self.buffers.pop(partition)
        first, last = buf.rows[0]["_offset"], buf.rows[-1]["_offset"]
        rows = [{k: r.get(k) for k in LANDED_SCHEMA.names} for r in buf.rows]
        sink = pa.BufferOutputStream()
        pq.write_table(pa.Table.from_pylist(rows, schema=LANDED_SCHEMA), sink, compression="zstd")
        path = landed_path(partition, first, last)
        self.store.put(path, bytes(sink.getvalue().to_pybytes()))
        self.consumer.commit(
            offsets=[TopicPartition(self.topic, partition, last + 1)], asynchronous=False
        )
        events = sum(r["kind"] == "EVENT" for r in buf.rows)
        self.conn.execute(
            b"insert into stream_hours (hour, partition, first_offset, last_offset, events, path) "
            b"values (%s, %s, %s, %s, %s, %s) on conflict (hour, partition) do update set "
            b"first_offset = excluded.first_offset, last_offset = excluded.last_offset, "
            b"events = excluded.events, path = excluded.path, landed_at = now()",
            (buf.hour, partition, first, last, events, path),
        )
        return {
            "hour": buf.hour,
            "partition": partition,
            "first": first,
            "last": last,
            "events": events,
        }


def read_landed(store: Store, path: str) -> pa.Table:
    return pq.read_table(io.BytesIO(store.get(path)))

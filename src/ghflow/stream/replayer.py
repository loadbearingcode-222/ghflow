"""Publish bronze hours to `gharchive.events`; the checkpoint moves only after the flush (ADR-0005).

For one hour: read its bronze Parquet, produce every row keyed by repo id (murmur2, so a repo's
events stay in one partition and in order), then one END_OF_HOUR marker to EVERY partition, then
flush. Only when the delivery reports confirm rows + markers, with no failure, does the hour's
checkpoint row get written. A replayer killed mid-hour therefore republishes the whole hour on
restart: consumers may see duplicates (counted, by event id), never a gap.
"""

from __future__ import annotations

import io
import time
from dataclasses import dataclass, field
from typing import Any

import psycopg
import pyarrow.parquet as pq
from confluent_kafka import KafkaError, Message, Producer

from ghflow import bronze, settings
from ghflow.hours import Hour
from ghflow.storage import Store
from ghflow.stream import clients, schema

TOPIC = clients.TOPIC

CHECKPOINT_SQL = b"""
create table if not exists replay_checkpoint (
    hour          text        primary key,
    hour_start    timestamptz not null,
    rows_in_hour  bigint      not null,
    markers       int         not null,
    delivered     bigint      not null,
    replays       int         not null default 1,
    replayed_at   timestamptz not null default now()
);
"""


@dataclass
class Deliveries:
    ok: int = 0
    failed: list[str] = field(default_factory=list)

    def __call__(self, err: KafkaError | None, _msg: Message) -> None:
        if err is None:
            self.ok += 1
        else:
            self.failed.append(str(err))


def _produce(producer: Producer, deliveries: Deliveries, **kwargs: Any) -> None:
    while True:
        try:
            producer.produce(TOPIC, on_delivery=deliveries, **kwargs)
            return
        except BufferError:  # the local queue is full: let deliveries drain it
            producer.poll(0.5)


def replay_hour(
    hour: Hour, store: Store, producer: Producer, serialize: schema.Serializer
) -> dict[str, Any]:
    """Publish one hour and its markers; returns counts. Raises if any delivery failed."""
    table = pq.read_table(io.BytesIO(store.get(bronze.bronze_path(hour))))
    rows = table.to_pylist()
    partitions = len(producer.list_topics(TOPIC, timeout=30).topics[TOPIC].partitions)
    deliveries, started = Deliveries(), time.monotonic()
    for row in rows:
        key = None if row["repo_id"] is None else str(row["repo_id"]).encode()
        _produce(producer, deliveries, key=key, value=serialize(schema.event_record(row, hour.key)))
        producer.poll(0)
    for partition in range(partitions):
        marker = schema.marker_record(hour.key, partition, len(rows))
        _produce(
            producer,
            deliveries,
            key=schema.MARKER_KEY,
            value=serialize(marker),
            partition=partition,
        )
    left = producer.flush(settings.STREAM_FLUSH_TIMEOUT_S)
    expected = len(rows) + partitions
    if left or deliveries.failed or deliveries.ok != expected:
        raise RuntimeError(
            f"{hour.key}: {deliveries.ok}/{expected} delivered, {left} still queued, "
            f"{len(deliveries.failed)} failed ({deliveries.failed[:3]}); checkpoint NOT moved"
        )
    return {"hour": hour.key, "rows": len(rows), "markers": partitions, "delivered": deliveries.ok,
            "seconds": round(time.monotonic() - started, 1)}  # fmt: skip


def checkpoint(conn: psycopg.Connection[Any], result: dict[str, Any], hour: Hour) -> None:
    """Called only after a fully confirmed hour; a replay of a checkpointed hour bumps `replays`."""
    conn.execute(
        b"insert into replay_checkpoint (hour, hour_start, rows_in_hour, markers, delivered) "
        b"values (%s, %s, %s, %s, %s) on conflict (hour) do update set rows_in_hour = excluded.rows_in_hour, "
        b"markers = excluded.markers, delivered = excluded.delivered, "
        b"replays = replay_checkpoint.replays + 1, replayed_at = now()",
        (hour.key, hour.start, result["rows"], result["markers"], result["delivered"]),
    )


def next_hour(conn: psycopg.Connection[Any], first: Hour) -> Hour:
    """The hour after the newest checkpoint (or `first`): hours are replayed in order."""
    row = conn.execute(b"select max(hour_start) from replay_checkpoint").fetchone()
    return first if not row or row[0] is None else max(first, Hour(row[0]).plus(1))

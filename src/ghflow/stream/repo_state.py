"""The repo-state processor: each repo's current name, as a compacted changelog (M3-S2, ADR-0009).

`gharchive.events` → `gharchive.repo_state`, keyed by repo id (the same murmur2 partition as the
repo's events). The unit of work is one (partition, hour), closed by the hour's END_OF_HOUR
marker, like the lander's (ADR-0006):

- Within the hour, a repo's name is the one on its latest event by (created_at, event id): the
  same rule as `dim_repo` (ADR-0008), so the hour's internal order and any duplicates from a
  replayed hour do not matter.
- At the marker, a **net change** is a repo whose hour-final name differs from the stored one
  and is newer than it (an old hour replayed by hand cannot move a repo back). Only net changes
  are written: A → B → A within one hour writes nothing.
- The changes and the offset past the marker commit in **one Kafka transaction**: both or
  neither. The store (SQLite, a rebuildable cache) is updated only after the commit.
- On start, the store is rebuilt from the changelog (committed records only), and consumption
  resumes at the last committed marker. A record is written only when the stored value would
  change, and the live store and a restored one hold the same values, so a restart re-derives
  the interrupted hour without emitting anything already committed.
"""

from __future__ import annotations

import io
import json
import sqlite3
import struct
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from fastavro import parse_schema, schemaless_reader, schemaless_writer

from ghflow.stream import clients
from ghflow.stream.state_store import StateStore, Stored, micros

TOPIC = clients.TOPIC
STATE_TOPIC = clients.REPO_STATE_TOPIC
SUBJECT = f"{STATE_TOPIC}-value"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

SCHEMA: dict[str, Any] = {
    "type": "record",
    "name": "RepoState",
    "namespace": "ghflow",
    "doc": "A repo's current name, written when it changes (ADR-0009). Key: the repo id.",
    "fields": [
        {"name": "repo_id", "type": "long"},
        {"name": "repo_name", "type": "string"},
        {
            "name": "as_of",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
            "doc": "created_at of the event that set this name",
        },
        {"name": "event_id", "type": "long", "doc": "that event's id (breaks same-second ties)"},
        {"name": "hour", "type": "string", "doc": "that event's GH Archive hour key"},
    ],
}
PARSED = parse_schema(SCHEMA)


def schema_json() -> str:
    return json.dumps(SCHEMA, indent=2) + "\n"


class Serializer:
    """Confluent wire format: magic byte 0, the 4-byte schema id, the Avro body."""

    def __init__(self, schema_id: int) -> None:
        self.prefix = b"\x00" + struct.pack(">I", schema_id)

    def __call__(self, record: Mapping[str, Any]) -> bytes:
        body = io.BytesIO()
        schemaless_writer(body, PARSED, dict(record))
        return self.prefix + body.getvalue()


def decode(value: bytes) -> dict[str, Any]:
    return schemaless_reader(io.BytesIO(value[5:]), PARSED, PARSED)  # type: ignore[return-value]


# ------------------------------------------------------------------ one (partition, hour)


@dataclass
class PartitionHour:
    hour: str
    latest: dict[int, tuple[datetime, int, str]] = field(default_factory=dict)
    events: int = 0

    def add(self, record: Mapping[str, Any]) -> None:
        self.events += 1
        repo, name = record.get("repo_id"), record.get("repo_name")
        if repo is None or name is None:
            return
        seen = (record["created_at"], record["id"], name)
        if repo not in self.latest or seen[:2] > self.latest[repo][:2]:
            self.latest[repo] = seen


def net_changes(buf: PartitionHour, stored: Mapping[int, Stored]) -> list[dict[str, Any]]:
    """The repos whose hour-final name differs from the stored one and is newer than it."""
    out = []
    for repo, (created, event_id, name) in sorted(buf.latest.items()):
        old = stored.get(repo)
        newer = old is None or (micros(created), event_id) > (old.as_of, old.event_id)
        if newer and (old is None or old.name != name):
            out.append({"repo_id": repo, "repo_name": name, "as_of": created,
                        "event_id": event_id, "hour": buf.hour})  # fmt: skip
    return out


class Processor:
    def __init__(
        self, consumer: Consumer, producer: Producer, store: StateStore, serialize: Serializer
    ) -> None:
        self.consumer, self.producer, self.store, self.serialize = (
            consumer,
            producer,
            store,
            serialize,
        )
        self.buffers: dict[int, PartitionHour] = {}

    def take(self, partition: int, offset: int, record: Mapping[str, Any]) -> dict[str, Any] | None:
        """Buffer one record; at its hour's marker, commit the partition-hour's net changes."""
        buf = self.buffers.setdefault(partition, PartitionHour(record["hour"]))
        if record["hour"] != buf.hour:
            raise RuntimeError(
                f"partition {partition}: {record['hour']} arrived before {buf.hour}'s marker; refusing to mix hours"
            )
        if record["kind"] == "EVENT":
            buf.add(record)
            return None
        return self.close(partition, offset)

    def close(self, partition: int, marker_offset: int) -> dict[str, Any]:
        buf = self.buffers.pop(partition)
        changes = net_changes(buf, self.store.lookup(buf.latest))
        self.producer.begin_transaction()
        for change in changes:
            # The events' partition, explicitly: a repo's state sits in its events' partition, and
            # the store knows which changelog partition this transaction moved.
            self._produce(str(change["repo_id"]).encode(), self.serialize(change), partition)
        self.producer.send_offsets_to_transaction(
            [TopicPartition(TOPIC, partition, marker_offset + 1)],
            self.consumer.consumer_group_metadata(),
        )
        self.producer.commit_transaction()
        # Only after the commit: a crash before this leaves the store behind, and the next start
        # reads the changelog from the older position. The position may lag (the commit markers
        # land asynchronously), never lead: only this processor writes the changelog.
        self.store.put(changes, self._ends({partition} if changes else set()))
        return {"hour": buf.hour, "partition": partition, "events": buf.events,
                "repos": len(buf.latest), "changes": len(changes)}  # fmt: skip

    def _ends(self, partitions: set[int]) -> dict[int, int]:
        ends = {}
        for p in sorted(partitions):
            tp = TopicPartition(STATE_TOPIC, p)
            ends[p] = self.consumer.get_watermark_offsets(tp, timeout=30, cached=False)[1]
        return ends

    def _produce(self, key: bytes, value: bytes, partition: int) -> None:
        while True:
            try:
                self.producer.produce(STATE_TOPIC, key=key, value=value, partition=partition)
                return
            except BufferError:  # the local queue is full: let deliveries drain it
                self.producer.poll(0.5)


# ------------------------------------------------------------------ restore


def read_to_end(
    reader: Consumer, topic: str, start: Mapping[int, int] | None = None, batch: int = 10_000
) -> Iterable[Any]:
    """Every readable message of `topic` from `start` (per partition; default 0) until EOF on
    every partition. The reader's isolation level decides what is readable."""
    partitions = reader.list_topics(topic, timeout=30).topics[topic].partitions
    begin = start or {}
    reader.assign([TopicPartition(topic, p, begin.get(p, 0)) for p in partitions])
    open_parts = set(partitions)
    while open_parts:
        for msg in reader.consume(batch, timeout=5.0):
            err = msg.error()
            if err is None:
                yield msg
            elif err.code() == KafkaError._PARTITION_EOF:
                open_parts.discard(msg.partition())
            else:
                raise RuntimeError(f"reading {topic}: {err}")


def ends(reader: Consumer, topic: str) -> dict[int, int]:
    """Per partition, the end a committed reader stops at."""
    partitions = reader.list_topics(topic, timeout=30).topics[topic].partitions
    return {
        p: reader.get_watermark_offsets(TopicPartition(topic, p), timeout=30, cached=False)[1]
        for p in partitions
    }


def restore(reader: Consumer, store: StateStore, start: Mapping[int, int] | None = None) -> int:
    """Apply the changelog's committed records from `start` (default: the beginning) to the
    store, and record where it ended; returns how many records were read."""
    end = ends(reader, STATE_TOPIC)  # taken first: the store may lag, never lead
    read, pending = 0, []
    for msg in read_to_end(reader, STATE_TOPIC, start):
        pending.append(decode(msg.value()))
        read += 1
        if len(pending) >= 50_000:
            store.put(pending)
            pending = []
    store.put(pending, end)
    return read


def open_store(path: Path) -> tuple[StateStore, dict[str, Any]]:
    """The store the processor starts from: the persisted one plus the changelog tail after its
    position, or (no store, an unreadable one, or a position past the topic's end) a full
    rebuild from the beginning. Returns the store and what was done."""
    started = time.monotonic()
    reader = clients.changelog_reader()
    try:
        mode, store, start = "tail", None, None
        try:
            store = StateStore(path)
            start = store.positions()
            end = ends(reader, STATE_TOPIC)
            if not start:
                mode = "full: no stored position"
            elif any(start.get(p, 0) > e for p, e in end.items()):
                mode = "full: stored position past the topic's end"
        except sqlite3.DatabaseError as exc:
            mode = f"full: unreadable store ({exc})"
        if mode != "tail":
            store, start = StateStore(path, fresh=True), None
        assert store is not None
        read = restore(reader, store, start)
    finally:
        reader.close()
    return store, {
        "mode": mode,
        "restored_records": read,
        "seconds": round(time.monotonic() - started, 1),
    }

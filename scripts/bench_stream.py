"""Stream benchmark (M2-S4): replayer and lander events/s on a fixed hour, disposable topics.

uv run python scripts/bench.py stream --runs 5 --out automation/runs/m2-s4/bench-stream.json
"""

from __future__ import annotations

import hashlib
import io
import statistics
import time
import uuid
from typing import Any

import confluent_kafka.admin as kafka_admin
from bench import summary

from ghflow.hours import Hour

#: The fixed hour for the stream benchmark (M2-S4): 165,375 events, published and landed per run.
STREAM_HOUR = "2026-08-03-0"


class _NoStore:
    """The lander's S3 PUT left out of the timing; keeps each file's sha256 as the fingerprint."""

    def __init__(self) -> None:
        self.shas: list[str] = []

    def put(self, path: str, data: bytes) -> str:
        sha = hashlib.sha256(data).hexdigest()
        self.shas.append(sha)
        return sha


class _NoConn:
    def execute(self, *_: object) -> None:
        return None


def _land_topic(topic: str, partitions: int) -> tuple[float, int, list[str]]:
    """Consume a bench topic from the start through the real Lander until every partition lands."""
    from confluent_kafka import OFFSET_BEGINNING, TopicPartition
    from fastavro import parse_schema, schemaless_reader

    from ghflow.stream import clients, lander, schema

    consumer = clients.peek_consumer()
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in range(partitions)])
    parsed = parse_schema(schema.schema())
    store = _NoStore()
    land = lander.Lander(consumer, store, _NoConn(), topic=topic)  # type: ignore[arg-type]
    landed, events, started = 0, 0, time.perf_counter()
    while landed < partitions:
        msg = consumer.poll(1.0)
        value = None if msg is None or msg.error() else msg.value()
        if msg is None or value is None:
            continue
        record = schemaless_reader(io.BytesIO(value[5:]), parsed, parsed)
        done = land.take(msg.partition() or 0, msg.offset() or 0, record)  # type: ignore[arg-type]
        if done:
            landed += 1
            events += done["events"]
    seconds = time.perf_counter() - started
    consumer.close()
    return seconds, events, sorted(store.shas)


def bench_stream(runs: int, _hours: list[str]) -> list[dict[str, Any]]:
    """Replayer and lander events/s on the fixed hour, each run on its own disposable topic.

    The real replay_hour publishes to a fresh 6-partition topic; the real Lander consumes it
    (its S3 PUT and Postgres row are left out of the timing); the topic is then deleted. The live
    gharchive.events is never touched.
    """
    from ghflow import settings
    from ghflow.commands.local import s3_config
    from ghflow.commands.stream import register
    from ghflow.storage import Store
    from ghflow.stream import clients, replayer, schema

    store, admin = Store(s3_config()), clients.admin()
    serializer = schema.Serializer(register(clients.registry()))
    hour = Hour.parse(STREAM_HOUR)
    produce_s, land_s, counts, fingerprints = [], [], [], set()
    for _ in range(runs):
        topic = f"bench.events.{uuid.uuid4().hex[:8]}"
        new_topic = kafka_admin.NewTopic  # type: ignore[attr-defined]  # real; the 2.16 stub omits it
        admin.create_topics([new_topic(topic, settings.STREAM_PARTITIONS, 1)])[topic].result(
            timeout=30
        )
        try:
            producer = clients.producer()
            started = time.perf_counter()
            published = replayer.replay_hour(hour, store, producer, serializer, topic)
            produce_s.append(time.perf_counter() - started)
            seconds, events, shas = _land_topic(topic, settings.STREAM_PARTITIONS)
            fingerprints.add(hashlib.sha256("".join(shas).encode()).hexdigest())
            land_s.append(seconds)
            counts.append((published["delivered"], events))
        finally:
            admin.delete_topics([topic])[topic].result(timeout=30)
    rows = counts[0][1]
    return [
        {
            "hour": STREAM_HOUR,
            "events": rows,
            "delivered_and_landed_every_run": sorted(set(counts)),
            "landed_files_fingerprint": sorted(fingerprints),
            "linger_ms": settings.STREAM_LINGER_MS,
            "batch_bytes": settings.STREAM_BATCH_BYTES,
            "excluded": "the lander's S3 PUT and Postgres row (disk and network, not code)",
            "replayer": summary(produce_s, rows)
            | {"events_per_s": round(rows / statistics.median(produce_s))},
            "lander": summary(land_s, rows)
            | {"events_per_s": round(rows / statistics.median(land_s))},
        }
    ]

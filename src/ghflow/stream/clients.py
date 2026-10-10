"""The one Kafka and Schema Registry client configuration (M2-S4, ADR-0005).

Every producer, consumer and admin client in ghflow is built here; `tests/unit/test_laws.py`
refuses a client setting (`bootstrap.servers`, `group.id`, `acks`, ...) written anywhere else, so
a delivery guarantee cannot be weakened in one corner by accident. Endpoints come from the
environment (compose sets them for the services) with host defaults for the CLIs.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from ghflow import settings

TOPIC = "gharchive.events"
LANDER_GROUP = "lander"


def bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:39092")


def registry() -> str:
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://127.0.0.1:38081")


def producer_config() -> dict[str, Any]:
    """Publishing: no loss (acks all), no duplicates from retries (idempotence), keyed order."""
    return {
        "bootstrap.servers": bootstrap(),
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "zstd",
        "linger.ms": settings.STREAM_LINGER_MS,
        "batch.size": settings.STREAM_BATCH_BYTES,
        "partitioner": "murmur2_random",  # Java-compatible: same repo id, same partition
        "queue.buffering.max.messages": 500_000,
    }


def lander_config() -> dict[str, Any]:
    """Landing: commits only by hand, after the file is written (ADR-0006)."""
    return {
        "bootstrap.servers": bootstrap(),
        "group.id": LANDER_GROUP,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 600_000,
    }


def producer() -> Producer:
    return Producer(producer_config())


def lander_consumer() -> Consumer:
    return Consumer(lander_config())


def peek_consumer(group: str | None = None) -> Consumer:
    """A consumer that never commits: for verify, lag and coverage (reads, never moves a group)."""
    return Consumer(
        {
            "bootstrap.servers": bootstrap(),
            "group.id": group or f"peek-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )


def admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap()})

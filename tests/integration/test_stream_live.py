"""The stream platform as applied: the topic matches its spec, the registry guards the schema."""

from __future__ import annotations

import json
import urllib.request

import confluent_kafka.admin as kafka_admin
import pytest

from ghflow import settings
from ghflow.stream import schema

pytestmark = pytest.mark.integration

BOOTSTRAP, REGISTRY, TOPIC = "127.0.0.1:39092", "http://127.0.0.1:38081", "gharchive.events"


def test_the_topic_matches_its_spec() -> None:
    admin = kafka_admin.AdminClient({"bootstrap.servers": BOOTSTRAP})
    topic = admin.list_topics(TOPIC, timeout=30).topics[TOPIC]
    assert len(topic.partitions) == settings.STREAM_PARTITIONS
    # ConfigResource is real; the 2.16 type stub leaves it out of __all__.
    config_resource = kafka_admin.ConfigResource  # type: ignore[attr-defined]
    resource = config_resource(config_resource.Type.TOPIC, TOPIC)
    configs = admin.describe_configs([resource])[resource].result(timeout=30)
    assert int(configs["retention.ms"].value) == settings.STREAM_RETENTION_DAYS * 24 * 3600 * 1000
    assert configs["message.timestamp.type"].value == "LogAppendTime"


def test_the_registry_holds_the_schema_under_backward() -> None:
    def get(path: str) -> object:
        with urllib.request.urlopen(REGISTRY + path, timeout=10) as response:
            return json.load(response)

    assert get(f"/config/{schema.SUBJECT}") == {"compatibilityLevel": "BACKWARD"}
    latest = get(f"/subjects/{schema.SUBJECT}/versions/latest")
    assert isinstance(latest, dict) and json.loads(latest["schema"]) == schema.schema()

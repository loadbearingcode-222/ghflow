"""The stream record: generated from bronze, refused by named rule, BACKWARD-checked offline."""

from __future__ import annotations

import copy
import io
from datetime import UTC, datetime

import pytest
from fastavro import parse_schema, schemaless_reader

from ghflow import bronze
from ghflow.commands.stream import AVSC
from ghflow.stream import schema


def event(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {name: None for name in bronze.SCHEMA.names}
    row |= {"id": 40_000_000_001, "type": "PushEvent", "repo_id": 2, "actor_id": 1,
            "created_at": datetime(2026, 8, 3, 0, 7, tzinfo=UTC)}  # fmt: skip
    return schema.event_record({**row, **overrides}, "2026-08-03-0")


def test_the_committed_avsc_is_exactly_the_generated_schema() -> None:
    assert AVSC.read_text(encoding="utf-8") == schema.schema_json(), (
        "run: python -m ghflow.commands.stream schema"
    )


def test_every_bronze_column_is_carried_plus_kind_hour_and_marker_fields() -> None:
    names = [f["name"] for f in schema.schema()["fields"]]
    assert names[:4] == ["kind", "hour", "partition", "rows_in_hour"]
    assert names[4:] == bronze.SCHEMA.names


def test_an_event_round_trips_through_the_wire_format() -> None:
    wire = schema.Serializer(7)(event())
    assert wire[:5] == b"\x00\x00\x00\x00\x07", "magic byte 0, then the schema id"
    parsed = parse_schema(schema.schema())
    back = schemaless_reader(io.BytesIO(wire[5:]), parsed, parsed)
    assert back["id"] == 40_000_000_001 and back["kind"] == "EVENT"  # type: ignore[call-overload,index]


@pytest.mark.parametrize(
    ("missing", "rule"),
    [("id", "EVENT_ID_REQUIRED"), ("type", "EVENT_TYPE_REQUIRED"),
     ("created_at", "EVENT_CREATED_AT_REQUIRED"), ("repo_id", "EVENT_REPO_ID_REQUIRED")],
)  # fmt: skip
def test_a_record_missing_a_required_field_is_refused_by_its_rule(missing: str, rule: str) -> None:
    with pytest.raises(schema.RecordRefusal) as refusal:
        schema.Serializer(1)(event(**{missing: None}))
    assert refusal.value.rule == rule


def test_a_marker_needs_its_partition() -> None:
    schema.Serializer(1)(schema.marker_record("2026-08-03-0", 3, 165_375))
    with pytest.raises(schema.RecordRefusal) as refusal:
        schema.Serializer(1)({"kind": "END_OF_HOUR", "hour": "2026-08-03-0", "partition": None})
    assert refusal.value.rule == "MARKER_PARTITION_REQUIRED"


def test_the_schema_is_backward_compatible_with_itself() -> None:
    assert schema.backward_problems(schema.schema(), schema.schema()) == []


def test_adding_a_field_needs_a_default() -> None:
    new = schema.schema()
    new["fields"].append({"name": "added", "type": "string"})
    assert schema.backward_problems(new, schema.schema()) == ["added: added without a default"]
    new["fields"][-1] = {"name": "added", "type": ["null", "string"], "default": None}
    assert schema.backward_problems(new, schema.schema()) == []


def test_changing_a_type_or_dropping_an_enum_symbol_is_refused() -> None:
    old = schema.schema()
    retyped = copy.deepcopy(old)
    retyped["fields"] = [
        f if f["name"] != "repo_id" else {"name": "repo_id", "type": "string"}
        for f in retyped["fields"]
    ]
    assert schema.backward_problems(retyped, old) == ["repo_id: cannot read the old type"]
    narrowed = copy.deepcopy(old)
    narrowed["fields"][0]["type"]["symbols"] = ["EVENT"]
    assert schema.backward_problems(narrowed, old) == ["kind: cannot read the old type"]
    widened = copy.deepcopy(old)
    widened["fields"][0]["type"]["symbols"] = ["EVENT", "END_OF_HOUR", "HEARTBEAT"]
    assert schema.backward_problems(widened, old) == []

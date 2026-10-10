"""The stream's record: an Avro schema generated from bronze, named rules, wire format. Pure.

One shape, declared once: every field of bronze (`ghflow.bronze.SCHEMA`) becomes a nullable Avro
field, plus `kind` (an EVENT or an END_OF_HOUR marker), `hour` and the marker's `partition`.
`config/schemas/gh_event.avsc` is this schema, written by `python -m ghflow.commands.stream
schema` and checked for drift by a unit test, like the silver models (ADR-0005).

Avro cannot say "required when kind = EVENT", so the serializer checks named rules first and
refuses a record by the rule it breaks. BACKWARD compatibility (a new reader reads old data) is
checked offline by `backward_problems`, in CI against the schema on main, and online by the
registry.
"""

from __future__ import annotations

import io
import json
import struct
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import pyarrow as pa
from fastavro import parse_schema, schemaless_writer

from ghflow import bronze

SUBJECT = "gharchive.events-value"
MARKER_KEY = b"__end_of_hour__"
_ARROW_TO_AVRO = {
    pa.int64(): "long",
    pa.string(): "string",
    pa.bool_(): "boolean",
}


class RecordRefusal(ValueError):
    """A record breaks a named rule; it is never serialized."""

    def __init__(self, rule: str, detail: str) -> None:
        super().__init__(f"{rule}: {detail}")
        self.rule = rule


def _avro_type(arrow: pa.DataType) -> Any:
    if pa.types.is_timestamp(arrow):
        return {"type": "long", "logicalType": "timestamp-micros"}
    return _ARROW_TO_AVRO[arrow]


def schema() -> dict[str, Any]:
    """The Avro schema of `gharchive.events`, derived from bronze's Parquet schema."""
    fields: list[dict[str, Any]] = [
        {
            "name": "kind",
            "type": {"type": "enum", "name": "Kind", "symbols": ["EVENT", "END_OF_HOUR"]},
        },
        {"name": "hour", "type": "string", "doc": "GH Archive hour key, e.g. 2026-08-03-0"},
        {
            "name": "partition",
            "type": ["null", "int"],
            "default": None,
            "doc": "END_OF_HOUR: the partition it closes",
        },
        {
            "name": "rows_in_hour",
            "type": ["null", "long"],
            "default": None,
            "doc": "END_OF_HOUR: the hour's bronze rows",
        },
    ]
    for f in bronze.SCHEMA:
        fields.append({"name": f.name, "type": ["null", _avro_type(f.type)], "default": None})
    return {
        "type": "record",
        "name": "GhEvent",
        "namespace": "ghflow",
        "doc": "One GH Archive event from bronze, or an end-of-hour marker (ADR-0005). Generated; do not edit.",
        "fields": fields,
    }


def schema_json() -> str:
    return json.dumps(schema(), indent=2) + "\n"


#: Rules a record must meet before it is serialized, in order: the first broken one is named.
EVENT_RULES = (
    ("EVENT_ID_REQUIRED", "id"),
    ("EVENT_TYPE_REQUIRED", "type"),
    ("EVENT_CREATED_AT_REQUIRED", "created_at"),
    ("EVENT_REPO_ID_REQUIRED", "repo_id"),
)


def check(record: Mapping[str, Any]) -> None:
    """Raise RecordRefusal naming the first rule the record breaks."""
    if not record.get("hour"):
        raise RecordRefusal("RECORD_HOUR_REQUIRED", "every record names its hour")
    if record.get("kind") == "END_OF_HOUR":
        if record.get("partition") is None:
            raise RecordRefusal("MARKER_PARTITION_REQUIRED", f"marker for {record['hour']}")
        return
    if record.get("kind") != "EVENT":
        raise RecordRefusal("RECORD_KIND_UNKNOWN", repr(record.get("kind")))
    for rule, field in EVENT_RULES:
        if record.get(field) is None:
            raise RecordRefusal(rule, f"{record['hour']} event id={record.get('id')}")


class Serializer:
    """Confluent wire format: magic byte 0, the 4-byte schema id, the Avro body."""

    def __init__(self, schema_id: int) -> None:
        self.schema_id = schema_id
        self.parsed = parse_schema(schema())
        self.prefix = b"\x00" + struct.pack(">I", schema_id)

    def __call__(self, record: Mapping[str, Any]) -> bytes:
        check(record)
        body = io.BytesIO()
        schemaless_writer(body, self.parsed, dict(record))
        return self.prefix + body.getvalue()


def event_record(row: Mapping[str, Any], hour_key: str) -> dict[str, Any]:
    """A bronze row as a stream record (created_at as a timestamp, as bronze typed it)."""
    record = {"kind": "EVENT", "hour": hour_key, **row}
    created = record.get("created_at")
    if created is not None and not isinstance(created, datetime):
        raise RecordRefusal(
            "EVENT_CREATED_AT_TYPED", f"{hour_key}: created_at is {type(created).__name__}"
        )
    return record


def marker_record(hour_key: str, partition: int, rows_in_hour: int) -> dict[str, Any]:
    return {
        "kind": "END_OF_HOUR",
        "hour": hour_key,
        "partition": partition,
        "rows_in_hour": rows_in_hour,
    }


# ------------------------------------------------------------------ BACKWARD, offline

_PROMOTIONS = {("int", "long"), ("int", "float"), ("int", "double"), ("long", "float"),
               ("long", "double"), ("float", "double"), ("string", "bytes"), ("bytes", "string")}  # fmt: skip


def _branches(t: Any) -> list[Any]:
    return t if isinstance(t, list) else [t]


def _name(t: Any) -> str:
    return (
        t["type"]
        if isinstance(t, dict) and not t.get("logicalType")
        else json.dumps(t, sort_keys=True)
    )


def _enum(t: Any) -> dict[str, Any] | None:
    return t if isinstance(t, dict) and t.get("type") == "enum" else None


def _reads_branch(new: Any, branch: Any) -> bool:
    """Can a reader of (union) type `new` read one written branch?"""
    if old_enum := _enum(branch):
        new_enum = next((e for b in _branches(new) if (e := _enum(b))), None)
        return bool(new_enum) and set(old_enum["symbols"]) <= set(new_enum["symbols"])  # type: ignore[index]
    readable = {_name(b) for b in _branches(new) if not _enum(b)}
    written = _name(branch)
    return written in readable or any((written, r) in _PROMOTIONS for r in readable)


def _reads(new: Any, old: Any) -> bool:
    """Can a reader of type `new` read every value written as `old`?"""
    return all(_reads_branch(new, branch) for branch in _branches(old))


def backward_problems(new: Mapping[str, Any], old: Mapping[str, Any]) -> list[str]:
    """Why a reader with `new` could not read data written with `old` (empty: compatible)."""
    old_fields = {f["name"]: f for f in old["fields"]}
    problems = []
    for f in new["fields"]:
        if f["name"] not in old_fields:
            if "default" not in f:
                problems.append(f"{f['name']}: added without a default")
        elif not _reads(f["type"], old_fields[f["name"]]["type"]):
            problems.append(f"{f['name']}: cannot read the old type")
    return problems

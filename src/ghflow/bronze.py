"""Bronze: the contract's rows as Parquet, and where every object of an hour lives. Pure.

Bronze is *projected* (ADR-0002): the envelope plus the typed payload fields the contract
declares, never free-text bodies. Each hour is one Parquet file, plus a rejected-row sidecar
that is written even when empty, so "rows in = bronze rows + sidecar rows" can be checked from
storage alone.

Encoding is deterministic: a fixed schema, fixed compression and one row group, written to an
in-memory buffer. The same rows always produce the same bytes, which is what lets a re-run prove
itself by sha256 (M1-S1).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ghflow import settings
from ghflow.fields import ENVELOPE_COLUMNS, FIELDS
from ghflow.hours import Hour

RAW_BUCKET = "raw"
BRONZE_BUCKET = "bronze"

_KIND_TYPES = {"int": pa.int64(), "str": pa.string(), "bool": pa.bool_(), "count": pa.int64()}
_ENVELOPE_TYPES = {
    "id": pa.int64(),
    "type": pa.string(),
    "created_at": pa.timestamp("us", tz="UTC"),
    "actor_id": pa.int64(),
    "actor_login": pa.string(),
    "repo_id": pa.int64(),
    "repo_name": pa.string(),
    "org_id": pa.int64(),
    "org_login": pa.string(),
    "public": pa.bool_(),
}


def _payload_types() -> dict[str, pa.DataType]:
    types: dict[str, pa.DataType] = {}
    for f in FIELDS:
        kind = _KIND_TYPES[f.kind]
        if types.setdefault(f.name, kind) != kind:
            raise TypeError(f"payload column {f.name} is declared with two kinds")
    return types


SCHEMA = pa.schema(
    [pa.field(name, _ENVELOPE_TYPES[name]) for name in ENVELOPE_COLUMNS]
    + [pa.field(name, kind) for name, kind in _payload_types().items()]
)
SIDECAR_SCHEMA = pa.schema(
    [pa.field("line", pa.int64()), pa.field("rule", pa.string()), pa.field("raw", pa.string())]
)


# ------------------------------------------------------------------ where an hour lives


def _partition(hour: Hour) -> str:
    return f"date={hour.day.isoformat()}/hour={hour.start.hour:02d}"


def raw_path(hour: Hour) -> str:
    """The archive's file, exactly as served."""
    return f"{RAW_BUCKET}/gharchive/date={hour.day.isoformat()}/{hour.key}.json.gz"


def bronze_path(hour: Hour) -> str:
    return f"{BRONZE_BUCKET}/gharchive/{_partition(hour)}/events.parquet"


def sidecar_path(hour: Hour) -> str:
    return f"{BRONZE_BUCKET}/gharchive_rejected/{_partition(hour)}/rejected.parquet"


# ------------------------------------------------------------------ encoding


def _encode(table: pa.Table, hour: Hour) -> bytes:
    table = table.replace_schema_metadata({"ghflow.hour": hour.key})
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        compression_level=settings.BRONZE_ZSTD_LEVEL,
        row_group_size=settings.BRONZE_ROW_GROUP_ROWS,
    )
    return bytes(sink.getvalue().to_pybytes())


def events_parquet(rows: Sequence[dict[str, Any]], hour: Hour) -> bytes:
    """The accepted rows, in file order, as one Parquet file."""
    return _encode(pa.Table.from_pylist(list(rows), schema=SCHEMA), hour)


def sidecar_parquet(rejected: Sequence[dict[str, Any]], hour: Hour) -> bytes:
    """Rejected rows (line number, first rule broken, the raw line); written even when empty."""
    return _encode(pa.Table.from_pylist(list(rejected), schema=SIDECAR_SCHEMA), hour)

"""Bronze encoding: deterministic bytes, a schema that matches the contract, and its paths."""

from __future__ import annotations

import io

import pyarrow.parquet as pq

from factories import HOUR, line, push
from ghflow import bronze
from ghflow.contract import apply
from ghflow.fields import COLUMNS
from ghflow.hours import Hour


def result_of(*bodies: dict[str, object]) -> list[dict[str, object]]:
    return apply([line(b) for b in bodies], HOUR).rows


def test_the_schema_is_exactly_the_contract_columns_in_order() -> None:
    assert tuple(bronze.SCHEMA.names) == COLUMNS


def test_the_same_rows_encode_to_the_same_bytes() -> None:
    rows = result_of(push(40_000_000_001), push(40_000_000_002))
    assert bronze.events_parquet(rows, HOUR) == bronze.events_parquet(rows, HOUR)


def test_different_rows_encode_differently() -> None:
    a = bronze.events_parquet(result_of(push(40_000_000_001)), HOUR)
    b = bronze.events_parquet(result_of(push(40_000_000_002)), HOUR)
    assert a != b


def test_bronze_reads_back_as_the_rows_typed() -> None:
    rows = result_of(push(40_000_000_001, ref="refs/heads/dev"))
    table = pq.read_table(io.BytesIO(bronze.events_parquet(rows, HOUR)))
    assert table.num_rows == 1
    back = table.to_pylist()[0]
    assert back["id"] == 40_000_000_001 and back["ref"] == "refs/heads/dev"
    assert back["created_at"] == rows[0]["created_at"]
    assert table.schema.metadata[b"ghflow.hour"] == HOUR.key.encode()


def test_an_empty_sidecar_is_still_a_valid_file() -> None:
    table = pq.read_table(io.BytesIO(bronze.sidecar_parquet([], HOUR)))
    assert table.num_rows == 0 and table.schema.names == ["line", "rule", "raw"]


def test_paths_partition_by_day_and_zero_padded_hour() -> None:
    hour = Hour.parse("2026-08-03-7")
    assert bronze.raw_path(hour) == "raw/gharchive/date=2026-08-03/2026-08-03-7.json.gz"
    assert bronze.bronze_path(hour) == "bronze/gharchive/date=2026-08-03/hour=07/events.parquet"
    assert bronze.sidecar_path(hour).endswith("date=2026-08-03/hour=07/rejected.parquet")

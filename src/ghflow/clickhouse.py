"""ClickHouse's tables, generated from bronze's schema (M4-S1, ADR-0010). Pure.

The record is declared once (`ghflow.bronze.SCHEMA`, from which the Avro schema is generated
too); this module turns it into the ordered DDL that `config/clickhouse/initdb/001_ghflow.sql`
holds (written by `python -m ghflow.commands.clickhouse ddl`, checked for drift by a unit test).

- `events_queue` / `repo_state_queue`: Kafka engines (AvroConfluent through the registry), each
  with its own consumer group. They hold nothing; materialized views move what they read.
- `events`: ReplacingMergeTree ordered by (hour, event id). Delivery is at-least-once, so a
  re-read block lands twice until a merge; `FINAL` (or `uniqExact(id)`) sees each event once.
- `hour_markers`: the END_OF_HOUR markers, filtered out of `events` as data and kept, so an hour
  is complete in ClickHouse when all partitions' markers are in.
- `events_hourly`: distinct event ids per (hour, type) as `uniqExact` states, so a duplicate
  block cannot inflate it: it must equal a recomputation from `events`.
- `repo_current`: the repo-state changelog, latest per repo by `as_of` (ReplacingMergeTree).
"""

from __future__ import annotations

import pyarrow as pa

from ghflow import bronze

DATABASE = "ghflow"
EVENTS_TOPIC = "gharchive.events"
STATE_TOPIC = "gharchive.repo_state"
BROKERS = "redpanda:9092"
REGISTRY = "http://redpanda:8081"
EVENTS_GROUP = "clickhouse"
STATE_GROUP = "clickhouse-repo-state"

#: Columns that are never null in an EVENT record (the serializer's named rules), so `events`
#: stores them without Nullable.
REQUIRED = {"id": "Int64", "type": "LowCardinality(String)", "created_at": "DateTime64(6, 'UTC')",
            "repo_id": "Int64"}  # fmt: skip


def _base_type(arrow: pa.DataType) -> str:
    if pa.types.is_timestamp(arrow):
        return "DateTime64(6, 'UTC')"
    return {pa.int64(): "Int64", pa.string(): "String", pa.bool_(): "Bool"}[arrow]


def _columns(nullable_all: bool) -> list[tuple[str, str]]:
    out = []
    for f in bronze.SCHEMA:
        if not nullable_all and f.name in REQUIRED:
            out.append((f.name, REQUIRED[f.name]))
        else:
            out.append((f.name, f"Nullable({_base_type(f.type)})"))
    return out


def _kafka(topic: str, group: str) -> str:
    return (
        f"ENGINE = Kafka SETTINGS kafka_broker_list = '{BROKERS}', kafka_topic_list = '{topic}', "
        f"kafka_group_name = '{group}', kafka_format = 'AvroConfluent', "
        f"format_avro_schema_registry_url = '{REGISTRY}', kafka_num_consumers = 1, "
        # A block of the default ~1M wide rows, pushed through three views, does not fit the
        # 2 GiB container (M4-S1): 64k rows per block.
        "kafka_max_block_size = 65536"
    )


def _cols(columns: list[tuple[str, str]]) -> str:
    return ",\n    ".join(f"`{name}` {kind}" for name, kind in columns)


def statements() -> list[str]:
    """Every statement, in order: targets before the queues, queues before the views."""
    db = DATABASE
    queue_cols = [("kind", "String"), ("hour", "String"), ("partition", "Nullable(Int32)"),
                  ("rows_in_hour", "Nullable(Int64)"), *_columns(nullable_all=True)]  # fmt: skip
    event_cols = [*_columns(nullable_all=False), ("hour", "LowCardinality(String)"),
                  ("kafka_partition", "UInt16"), ("kafka_offset", "UInt64")]  # fmt: skip
    projected = ", ".join(
        f"assumeNotNull(`{name}`) AS `{name}`" if name in REQUIRED else f"`{name}`"
        for name, _ in _columns(nullable_all=True)
    )
    state_cols = [("repo_id", "Int64"), ("repo_name", "String"),
                  ("as_of", "DateTime64(6, 'UTC')"), ("event_id", "Int64"), ("hour", "String")]  # fmt: skip
    return [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"CREATE TABLE IF NOT EXISTS {db}.events (\n    {_cols(event_cols)}\n)\n"
        "ENGINE = ReplacingMergeTree\nPARTITION BY toYYYYMM(created_at)\n"
        "ORDER BY (toStartOfHour(created_at), id)",
        f"CREATE TABLE IF NOT EXISTS {db}.hour_markers (\n    `hour` LowCardinality(String),\n"
        "    `partition` UInt16,\n    `rows_in_hour` UInt64,\n    `kafka_offset` UInt64\n)\n"
        "ENGINE = ReplacingMergeTree\nORDER BY (hour, partition)",
        f"CREATE TABLE IF NOT EXISTS {db}.events_hourly (\n    `hour_start` DateTime('UTC'),\n"
        "    `type` LowCardinality(String),\n    `ids` AggregateFunction(uniqExact, Int64)\n)\n"
        "ENGINE = AggregatingMergeTree\nORDER BY (hour_start, type)",
        f"CREATE TABLE IF NOT EXISTS {db}.repo_current (\n    {_cols(state_cols)}\n)\n"
        "ENGINE = ReplacingMergeTree(as_of)\nORDER BY repo_id",
        f"CREATE TABLE IF NOT EXISTS {db}.events_queue (\n    {_cols(queue_cols)}\n)\n"
        + _kafka(EVENTS_TOPIC, EVENTS_GROUP),
        f"CREATE TABLE IF NOT EXISTS {db}.repo_state_queue (\n    {_cols(state_cols)}\n)\n"
        + _kafka(STATE_TOPIC, STATE_GROUP),
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.events_mv TO {db}.events AS\n"
        f"SELECT {projected}, hour, _partition AS kafka_partition, _offset AS kafka_offset\n"
        f"FROM {db}.events_queue WHERE kind = 'EVENT'",
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.hour_markers_mv TO {db}.hour_markers AS\n"
        "SELECT hour, assumeNotNull(partition) AS partition, assumeNotNull(rows_in_hour) AS rows_in_hour,\n"
        f"    _offset AS kafka_offset\nFROM {db}.events_queue WHERE kind = 'END_OF_HOUR'",
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.events_hourly_mv TO {db}.events_hourly AS\n"
        "SELECT toStartOfHour(assumeNotNull(created_at)) AS hour_start, assumeNotNull(type) AS type,\n"
        f"    uniqExactState(assumeNotNull(id)) AS ids\nFROM {db}.events_queue WHERE kind = 'EVENT'\n"
        "GROUP BY hour_start, type",
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.repo_current_mv TO {db}.repo_current AS\n"
        f"SELECT * FROM {db}.repo_state_queue",
    ]


def ddl_file() -> str:
    header = (
        "-- Generated by `python -m ghflow.commands.clickhouse ddl` from ghflow.bronze.SCHEMA\n"
        "-- (M4-S1, ADR-0010). Do not edit; a unit test refuses drift. Runs once, on an empty\n"
        "-- data volume (the image's initdb); `... clickhouse apply` re-applies it idempotently.\n\n"
    )
    return header + ";\n\n".join(statements()) + ";\n"

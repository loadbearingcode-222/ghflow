"""ClickHouse's DDL (M4-S1): generated from bronze's schema, committed, and shaped as ADR-0010 says.

The live proofs (three witnesses, FINAL, views = events, repo_current = dim_repo, the kill drill)
run against the stack: `python -m ghflow.commands.clickhouse status|verify|repo-diff` and
`python -m ghflow.commands.reconcile run --clickhouse`.
"""

from __future__ import annotations

from ghflow import bronze, clickhouse
from ghflow.commands import clickhouse as cli
from ghflow.reconcile import NOT_USED, compare


def name(statement: str) -> str:
    words = statement.split()
    return words[words.index("EXISTS") + 1]


def test_the_committed_ddl_is_the_generated_one() -> None:
    assert cli.ddl(check_only=True) == 0, "run: uv run python -m ghflow.commands.clickhouse ddl"


def test_every_bronze_column_reaches_events() -> None:
    events = next(s for s in clickhouse.statements() if ".events (" in s)
    for f in bronze.SCHEMA:
        assert f"`{f.name}`" in events, f.name


def test_events_dedups_by_event_id_within_its_hour() -> None:
    events = next(s for s in clickhouse.statements() if ".events (" in s)
    assert "ENGINE = ReplacingMergeTree" in events
    assert "ORDER BY (toStartOfHour(created_at), id)" in events


def test_markers_are_filtered_out_of_events_and_kept_as_data() -> None:
    views = {name(s): s for s in clickhouse.statements() if "MATERIALIZED VIEW" in s}
    assert "WHERE kind = 'EVENT'" in views["ghflow.events_mv"]
    assert "WHERE kind = 'END_OF_HOUR'" in views["ghflow.hour_markers_mv"]
    assert "uniqExactState" in views["ghflow.events_hourly_mv"], "duplicates must not inflate it"


def test_each_kafka_engine_has_its_own_group_and_the_registry() -> None:
    queues = [s for s in clickhouse.statements() if "ENGINE = Kafka" in s]
    groups = {s.split("kafka_group_name = '")[1].split("'")[0] for s in queues}
    assert groups == {"clickhouse", "clickhouse-repo-state"}
    assert all("kafka_format = 'AvroConfluent'" in s and "schema_registry_url" in s for s in queues)


def test_targets_exist_before_the_views_that_fill_them() -> None:
    order = [name(s) for s in clickhouse.statements()[1:]]
    assert order.index("ghflow.events") < order.index("ghflow.events_mv")
    assert order.index("ghflow.events_queue") < order.index("ghflow.events_mv")


def test_three_witnesses_green_red_and_pending() -> None:
    assert compare("h", [1, 2], [1, 2], None, [2, 1]).status == "green"
    red = compare("h", [1, 2, 3], [1, 2, 3], None, [1, 3, 9])
    assert (red.status, red.ch_missing, red.ch_extra) == ("red", [2], [9])
    assert compare("h", [1, 2], [1, 2], None, None).status == "pending"
    two = compare("h", [1, 2], [1, 2], None, NOT_USED)
    assert (two.status, two.clickhouse_ids) == ("green", None), "two-witness runs are unchanged"

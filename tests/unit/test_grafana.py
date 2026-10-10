"""The ops dashboard as code (M4-S2): generated, committed, one lag panel per consumer group."""

from __future__ import annotations

import json

from ghflow.commands import grafana
from ghflow.commands.lag_sampler import GROUPS


def test_the_committed_dashboard_is_the_generated_one() -> None:
    assert grafana.main(["dashboard", "--check"]) == 0, (
        "run: python -m ghflow.commands.grafana dashboard"
    )


def test_every_consumer_group_has_its_own_lag_panel() -> None:
    titles = {p.title for p in grafana.PANELS}
    assert {f"Consumer lag · {group}" for group, _ in GROUPS} <= titles


def test_the_accept_when_panels_exist() -> None:
    titles = " | ".join(p.title for p in grafana.PANELS).lower()
    for needle in ("freshness", "consumer lag", "reconciliation", "flagged"):
        assert needle in titles, needle


def test_every_panel_queries_clickhouse_and_lays_out_on_the_grid() -> None:
    board = json.loads(grafana.dashboard_json())
    for panel in board["panels"]:
        assert panel["datasource"]["uid"] == "ghflow-clickhouse"
        assert panel["targets"][0]["rawSql"].strip().upper().startswith("SELECT")
        assert panel["gridPos"]["x"] + panel["gridPos"]["w"] <= 24
    assert board["editable"] is False, "dashboards change in code, not in the UI"

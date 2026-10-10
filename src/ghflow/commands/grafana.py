"""Grafana's ops dashboard, as code, and the gate that runs every panel (M4-S2, ADR-0011).

    python -m ghflow.commands.grafana dashboard [--check]   # write (or check) the dashboard JSON
    python -m ghflow.commands.grafana gate                  # every panel's query through Grafana

The panels are declared once, here. The gate posts each panel's query to Grafana's own query
API (so provisioning, the plugin and ClickHouse are all exercised) over the last 7 days and
fails, naming the panel, if any returns no rows.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.request
from dataclasses import dataclass
from typing import Any

from ghflow.commands.lag_sampler import GROUPS
from ghflow.commands.local import REPO_ROOT, secret
from ghflow.exitcodes import ExitCode, guarded

DASHBOARD = REPO_ROOT / "docker" / "grafana" / "dashboards" / "ghflow-ops.json"
DATASOURCE = {"type": "grafana-clickhouse-datasource", "uid": "ghflow-clickhouse"}
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://127.0.0.1:33000")


@dataclass(frozen=True)
class Panel:
    title: str
    kind: str  # "stat" | "timeseries" | "table"
    sql: str
    width: int = 12
    description: str = ""
    time_from: str | None = None  # a panel's own window (Grafana's relative time override)


def _lag(group: str, topic: str) -> Panel:
    return Panel(
        f"Consumer lag · {group}",
        "timeseries",
        "SELECT ts AS time, sum(lag) AS lag FROM ghflow.consumer_lag\n"
        f"WHERE `group` = '{group}' AND $__timeFilter(ts)\nGROUP BY time ORDER BY time",
        6,
        f"Messages {group} has not yet committed on {topic}, summed over partitions. Flat is "
        "healthy; a steady climb means the consumer stopped.",
        "3h",
    )


PANELS = [
    Panel("Ingest freshness (minutes since the newest ingested hour ended)", "stat",
          "SELECT dateDiff('minute', max(hour_start) + INTERVAL 1 HOUR, now()) AS minutes\n"
          "FROM ghflow.ops_ingest_audit WHERE outcome = 'accepted'", 6,
          "GH Archive publishes an hour a few minutes after it ends; above ~90 means ingest is stuck."),
    Panel("Reconciliation: hours by status (latest run)", "table",
          "SELECT status, count() AS hours FROM (\n"
          "  SELECT hour, argMax(status, checked_at) AS status FROM ghflow.ops_reconcile_results\n"
          "  WHERE checked_at > now() - INTERVAL 3 HOUR GROUP BY hour\n) GROUP BY status ORDER BY status", 6,
          "Batch = stream = ClickHouse per hour. Anything but green names its ids in reconcile_results."),
    Panel("Flagged hours per day (quality)", "timeseries",
          "SELECT toStartOfDay(hour_start) AS time, countIf(status = 'flagged') AS flagged,\n"
          "  countIf(status = 'shifted') AS shifted FROM (\n"
          "  SELECT hour, any(hour_start) AS hour_start, argMax(status, judged_at) AS status\n"
          "  FROM ghflow.ops_quality_verdicts GROUP BY hour\n"
          ") WHERE $__timeFilter(hour_start) GROUP BY time ORDER BY time", 6,
          "flagged: volume or PushEvent broke (shaded on the stakeholder dashboard); shifted: minor types moved."),
    Panel("Events per hour (ClickHouse)", "timeseries",
          "SELECT hour_start AS time, events FROM ghflow.events_hour_counts\n"
          "WHERE $__timeFilter(hour_start) ORDER BY time", 6,
          "Distinct events per hour, as the third witness holds them (refreshed every 5 minutes, M4-S3)."),
    *[_lag(group, topic) for group, topic in GROUPS],
    Panel("Flagged hours (latest verdict)", "table",
          "SELECT hour, hour_start FROM (\n"
          "  SELECT hour, any(hour_start) AS hour_start, argMax(status, judged_at) AS status\n"
          "  FROM ghflow.ops_quality_verdicts GROUP BY hour\n"
          ") WHERE status = 'flagged' ORDER BY hour_start DESC LIMIT 200", 24),
]  # fmt: skip


def _target(panel: Panel) -> dict[str, Any]:
    table = panel.kind in ("table", "stat")
    return {"refId": "A", "datasource": DATASOURCE, "editorType": "sql", "rawSql": panel.sql,
            "queryType": "table" if table else "timeseries", "format": 1 if table else 0}  # fmt: skip


def dashboard() -> dict[str, Any]:
    panels, x, y = [], 0, 0
    for n, panel in enumerate(PANELS, start=1):
        if x + panel.width > 24:
            x, y = 0, y + 8
        panels.append({"id": n, "title": panel.title, "type": panel.kind, "description": panel.description,
                       "datasource": DATASOURCE, "gridPos": {"x": x, "y": y, "w": panel.width, "h": 8},
                       "targets": [_target(panel)], **({"timeFrom": panel.time_from} if panel.time_from else {})})  # fmt: skip
        x += panel.width
    return {"uid": "ghflow-ops", "title": "ghflow · ops", "tags": ["ghflow", "M4"],
            "time": {"from": "now-7d", "to": "now"}, "refresh": "30s", "schemaVersion": 39,
            "editable": False, "panels": panels}  # fmt: skip


def dashboard_json() -> str:
    return json.dumps(dashboard(), indent=2) + "\n"


def rows_returned(panel: Panel) -> int:
    """Run one panel's query through Grafana; how many rows came back across its frames."""
    auth = base64.b64encode(f"admin:{secret('GRAFANA_ADMIN_PASSWORD')}".encode()).decode()
    body = json.dumps({"queries": [_target(panel)], "from": "now-7d", "to": "now"}).encode()
    request = urllib.request.Request(
        f"{GRAFANA_URL}/api/ds/query", body,
        {"Content-Type": "application/json", "Authorization": f"Basic {auth}"}, method="POST",
    )  # fmt: skip
    with urllib.request.urlopen(request, timeout=120) as response:
        result = json.load(response)["results"]["A"]
    if result.get("error"):
        raise RuntimeError(f"{panel.title}: {result['error']}")
    frames = result.get("frames", [])
    return sum(len(f["data"]["values"][0]) if f["data"]["values"] else 0 for f in frames)


PLANTED = Panel(
    "planted: a panel whose query returns nothing",
    "table",
    "SELECT ts FROM ghflow.consumer_lag WHERE `group` = 'no-such-group'",
)


def gate(plant: bool = False) -> dict[str, Any]:
    """With `plant`, one empty panel is added: the gate must name it, and only it."""
    panels = [*PANELS, PLANTED] if plant else PANELS
    counts = {p.title: rows_returned(p) for p in panels}
    empty = [title for title, n in counts.items() if n == 0]
    return {"panels": len(counts), "rows": counts, "empty": empty, "passed": not empty}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["dashboard", "gate"])
    parser.add_argument("--check", action="store_true", help="dashboard: refuse if it drifted")
    parser.add_argument("--plant", action="store_true", help="gate: add one empty panel")
    args = parser.parse_args(argv)

    def go() -> int:
        if args.command == "dashboard":
            if args.check:
                same = (
                    DASHBOARD.exists() and DASHBOARD.read_text(encoding="utf-8") == dashboard_json()
                )
                return ExitCode.PASSED if same else ExitCode.REFUSED
            DASHBOARD.write_bytes(dashboard_json().encode())
            return ExitCode.PASSED
        record = gate(args.plant)
        print(json.dumps(record, indent=2))
        if args.plant:
            return ExitCode.PASSED if record["empty"] == [PLANTED.title] else ExitCode.REFUSED
        return ExitCode.PASSED if record["passed"] else ExitCode.REFUSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

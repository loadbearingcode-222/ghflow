"""dashboard_render: rebuild the stakeholder dashboard after every marts export (M5-S2, ADR-0013).

Runs when dbt_build's `export` updates the marts asset. Builds the pages from the snapshots,
checks every number back against the marts, and publishes only a checked site. Refuses when the
snapshots are older than the build that triggered it (the asset event carries its start).
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, task

GHFLOW_PYTHON = os.environ.get("GHFLOW_PYTHON", "/opt/ghflow-venv/bin/python")
MARTS = Asset("s3://marts/latest/manifest.json")


@dag(
    schedule=[MARTS],
    start_date=pendulum.datetime(2026, 8, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ghflow", "dashboard", "M5"],
    doc_md=__doc__,
)
def dashboard_render() -> None:
    @task(execution_timeout=timedelta(minutes=30), retries=1)
    def render(**context: object) -> None:
        events = context.get("triggering_asset_events") or {}
        extras = [e.extra for evs in events.values() for e in evs]  # type: ignore[attr-defined]
        not_before = next((x["build_started"] for x in extras if x.get("build_started")), None)
        args = [GHFLOW_PYTHON, "-m", "ghflow.commands.dashboard", "render"]
        args += ["--not-before", not_before] if not_before else []
        done = subprocess.run(args, capture_output=True, text=True)
        print(done.stdout, end="")
        print(done.stderr, end="")
        if done.returncode != 0:
            raise RuntimeError(f"dashboard render exited {done.returncode}: see above")

    render()


dashboard_render()

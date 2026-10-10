"""dbt_build: bronze → silver → gold → marts in DuckDB, once a day (M1-S3, ADR-0003; marts M5-S1).

`dbt build` loads the newest days of bronze into the incremental staging table, refreshes the
typed silver views and runs every test: the enforced contracts, the era tests (a field outside
its era fails by name) and silver rows = bronze rows per hour. Trigger with conf
{"full_refresh": true} to rebuild the whole window from bronze.

DuckDB has exactly one writer: both tasks run in the one-slot `duckdb` pool, and nothing else
writes the warehouse file. `compact` then copies the warehouse into a fresh file, because DuckDB
never shrinks its own (#26).
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

GHFLOW_PYTHON = os.environ.get("GHFLOW_PYTHON", "/opt/ghflow-venv/bin/python")
DBT = str(Path(GHFLOW_PYTHON).with_name("dbt"))
PROJECT = "/opt/ghflow/dbt/ghflow"
#: Updated by `export`; dashboard_render runs on it (M5-S2).
MARTS = Asset("s3://marts/latest/manifest.json")
FULL_REFRESH = "{{ 'true' if (dag_run.conf or {}).get('full_refresh') else 'false' }}"


def run(args: list[str], what: str) -> None:
    """Run one step in the project venv; its output goes to the task log; non-zero fails."""
    done = subprocess.run(args, capture_output=True, text=True)
    print(done.stdout, end="")
    print(done.stderr, end="")
    if done.returncode != 0:
        raise RuntimeError(f"{what} exited {done.returncode}")


@dag(
    # Daily at 02:30 UTC, after the day's last hour is published and ingested.
    schedule=CronDataIntervalTimetable("30 2 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 8, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ghflow", "batch", "M1"],
    doc_md=__doc__,
)
def dbt_build() -> None:
    @task(pool="duckdb", execution_timeout=timedelta(hours=3), retries=1)
    def build(full_refresh: str) -> None:
        args = [DBT, "build", "--project-dir", PROJECT, "--profiles-dir", PROJECT]
        run(args + (["--full-refresh"] if full_refresh == "true" else []), "dbt build")

    @task(pool="duckdb", execution_timeout=timedelta(hours=1), retries=0)
    def compact() -> None:
        """Copy the warehouse into a fresh file: DuckDB never shrinks its own (#26)."""
        run([GHFLOW_PYTHON, "-m", "ghflow.commands.warehouse", "compact"], "warehouse compact")

    @task(pool="duckdb", execution_timeout=timedelta(hours=1), retries=0)
    def twin() -> None:
        """M5-S1: every KPI headline recomputed independently; a mismatch stops the export."""
        run([GHFLOW_PYTHON, "-m", "ghflow.commands.marts", "twin"], "marts twin")

    @task(pool="duckdb", execution_timeout=timedelta(hours=1), retries=0, outlets=[MARTS])
    def export(**context: object) -> None:
        """Snapshot the marts (refused if any is empty, M5-S1). The asset event carries this
        build's start, so dashboard_render can refuse marts older than it (M5-S2)."""
        run([GHFLOW_PYTHON, "-m", "ghflow.commands.marts", "export"], "marts export")
        started = context["dag_run"].start_date.isoformat()  # type: ignore[attr-defined]
        context["outlet_events"][MARTS].extra = {"build_started": started}  # type: ignore[index]

    build(full_refresh=FULL_REFRESH) >> compact() >> twin() >> export()


dbt_build()

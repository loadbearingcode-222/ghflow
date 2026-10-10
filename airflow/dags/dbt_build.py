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
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

GHFLOW_PYTHON = os.environ.get("GHFLOW_PYTHON", "/opt/ghflow-venv/bin/python")
DBT = str(Path(GHFLOW_PYTHON).with_name("dbt"))
PROJECT = "/opt/ghflow/dbt/ghflow"
FULL_REFRESH = "{{ 'true' if (dag_run.conf or {}).get('full_refresh') else 'false' }}"


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
        if full_refresh == "true":
            args.append("--full-refresh")
        done = subprocess.run(args, capture_output=True, text=True)
        print(done.stdout, end="")
        print(done.stderr, end="")
        if done.returncode != 0:
            raise RuntimeError(f"dbt build exited {done.returncode}")

    @task(pool="duckdb", execution_timeout=timedelta(hours=1), retries=0)
    def compact() -> None:
        """Copy the warehouse into a fresh file: DuckDB never shrinks its own (#26)."""
        done = subprocess.run(
            [GHFLOW_PYTHON, "-m", "ghflow.commands.warehouse", "compact"],
            capture_output=True,
            text=True,
        )
        print(done.stdout, end="")
        print(done.stderr, end="")
        if done.returncode != 0:
            raise RuntimeError(f"warehouse compact exited {done.returncode}")

    @task(pool="duckdb", execution_timeout=timedelta(hours=1), retries=0)
    def marts(command: str) -> None:
        """M5-S1: `twin` recomputes every KPI headline independently and must match; `export`
        then snapshots the marts to s3://marts/latest (refused if any mart is empty)."""
        done = subprocess.run(
            [GHFLOW_PYTHON, "-m", "ghflow.commands.marts", command],
            capture_output=True,
            text=True,
        )
        print(done.stdout, end="")
        print(done.stderr, end="")
        if done.returncode != 0:
            raise RuntimeError(f"marts {command} exited {done.returncode}")

    (
        build(full_refresh=FULL_REFRESH)
        >> compact()
        >> marts.override(task_id="twin")("twin")
        >> marts.override(task_id="export")("export")
    )


dbt_build()

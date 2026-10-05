"""dbt_build: bronze → silver in DuckDB, once a day (M1-S3, ADR-0003).

`dbt build` loads the newest days of bronze into the incremental staging table, refreshes the
typed silver views and runs every test: the enforced contracts, the era tests (a field outside
its era fails by name) and silver rows = bronze rows per hour. Trigger with conf
{"full_refresh": true} to rebuild the whole window from bronze.

DuckDB has exactly one writer: this task runs in the one-slot `duckdb` pool, and nothing else
writes the warehouse file.
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

DBT = str(Path(os.environ.get("GHFLOW_PYTHON", "/opt/ghflow-venv/bin/python")).with_name("dbt"))
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

    build(full_refresh=FULL_REFRESH)


dbt_build()

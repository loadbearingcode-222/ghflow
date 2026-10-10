"""dbt_build: bronze → silver → gold → marts in DuckDB, once a day (M1-S3, ADR-0003; marts M5-S1).

`dbt build` loads the newest days of bronze into the incremental staging table, refreshes the
typed silver views and runs every test: the enforced contracts, the era tests (a field outside
its era fails by name) and silver rows = bronze rows per hour. Trigger with conf
{"full_refresh": true} to rebuild the whole window from bronze.

DuckDB has exactly one writer: both tasks run in the one-slot `duckdb` pool, and nothing else
writes the warehouse file. `compact` then copies the warehouse into a fresh file, because DuckDB
never shrinks its own (#26), and emits the warehouse asset: gharchive_quality judges the new
hours, then marts_publish rebuilds the marts with those verdicts (M5-S3).
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
#: Emitted after compact: gharchive_quality judges the new hours, then marts_publish (M5-S3).
WAREHOUSE = Asset("duckdb://warehouse/ghflow")
FULL_REFRESH = "{{ 'true' if (dag_run.conf or {}).get('full_refresh') else 'false' }}"


def run(args: list[str], what: str) -> None:
    """Run one step in the project venv; its output goes to the task log; non-zero fails."""
    done = subprocess.run(args, capture_output=True, text=True)
    print(done.stdout, end="")
    print(done.stderr, end="")
    if done.returncode != 0:
        raise RuntimeError(f"{what} exited {done.returncode}")


@dag(
    # Daily at 00:30 UTC: GH Archive publishes an hour 5.1-5.4 min after it ends (1,652 hours
    # measured, M5-S3), so the day's last hour has been ingested for ~20 min. Was 02:30.
    schedule=CronDataIntervalTimetable("30 0 * * *", timezone="UTC"),
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

    @task(pool="duckdb", execution_timeout=timedelta(hours=1), retries=0, outlets=[WAREHOUSE])
    def compact() -> None:
        """Copy the warehouse into a fresh file: DuckDB never shrinks its own (#26)."""
        run([GHFLOW_PYTHON, "-m", "ghflow.commands.warehouse", "compact"], "warehouse compact")

    build(full_refresh=FULL_REFRESH) >> compact()


dbt_build()

"""marts_publish: the marts, rebuilt with the newest verdicts, twinned and exported (M5-S3).

Runs when gharchive_quality's judge emits the verdicts asset, so a day reaches the dashboard
only once its hours are judged (before M5-S3 the marts were built at 02:30 and judged at 03:30:
the newest day's broken hours counted as clean until the next build). Then: every KPI headline
recomputed independently (a mismatch stops here), and the snapshots exported; the marts asset
then triggers dashboard_render. All in the one-slot `duckdb` pool (one writer, ADR-0003).
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, task

GHFLOW_PYTHON = os.environ.get("GHFLOW_PYTHON", "/opt/ghflow-venv/bin/python")
DBT = str(Path(GHFLOW_PYTHON).with_name("dbt"))
PROJECT = "/opt/ghflow/dbt/ghflow"
VERDICTS = Asset("postgres://platform-db:5432/ghflow_ops/public/quality_verdicts")
MARTS = Asset("s3://marts/latest/manifest.json")


def run(args: list[str], what: str) -> None:
    """Run one step in the project venv; its output goes to the task log; non-zero fails."""
    done = subprocess.run(args, capture_output=True, text=True)
    print(done.stdout, end="")
    print(done.stderr, end="")
    if done.returncode != 0:
        raise RuntimeError(f"{what} exited {done.returncode}")


@dag(
    schedule=[VERDICTS],
    start_date=pendulum.datetime(2026, 8, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ghflow", "marts", "M5"],
    doc_md=__doc__,
)
def marts_publish() -> None:
    @task(pool="duckdb", execution_timeout=timedelta(minutes=30), retries=1)
    def refresh() -> None:
        """The verdict model and everything downstream of it: the marts."""
        args = [DBT, "build", "--project-dir", PROJECT, "--profiles-dir", PROJECT]
        run([*args, "--select", "stg_ops__hour_verdicts+"], "dbt build (marts)")

    @task(pool="duckdb", execution_timeout=timedelta(hours=1), retries=0)
    def twin() -> None:
        run([GHFLOW_PYTHON, "-m", "ghflow.commands.marts", "twin"], "marts twin")

    @task(pool="duckdb", execution_timeout=timedelta(hours=1), retries=0, outlets=[MARTS])
    def export(**context: object) -> None:
        """The asset event carries this run's start: dashboard_render refuses older marts."""
        run([GHFLOW_PYTHON, "-m", "ghflow.commands.marts", "export"], "marts export")
        started = context["dag_run"].start_date.isoformat()  # type: ignore[attr-defined]
        context["outlet_events"][MARTS].extra = {"build_started": started}  # type: ignore[index]

    refresh() >> twin() >> export()


marts_publish()

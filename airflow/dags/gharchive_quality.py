"""gharchive_quality: judge every hour, re-check ETags, re-ingest what changed (M1-S4, ADR-0004).

After every dbt_build (the warehouse asset its compact emits; M5-S3, before that daily 03:30):
1. judge: every hour against its rolling clean baseline; each verdict, with the bars it was
   taken under, goes to ghflow_ops.quality_verdicts (the one table "broken" is defined by).
2. etag_check: HEAD the last 14 days of hours; a changed ETag queues the hour for re-ingest.
3. reingest: re-run ingest for queued hours (requests from the real audit table only).
4. If anything was re-ingested, dbt_build runs with full_refresh: its incremental model only
   re-reads the newest days (gotcha #48).
"""

from __future__ import annotations

import os
import subprocess

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Asset, dag, task

GHFLOW_PYTHON = os.environ.get("GHFLOW_PYTHON", "/opt/ghflow-venv/bin/python")
# Exit codes, as ghflow.exitcodes defines them (this interpreter cannot import ghflow).
PASSED, NO_VERDICT = 0, 3
#: M5-S3 chain: dbt_build -> (WAREHOUSE) -> judge -> (VERDICTS) -> marts_publish -> dashboard_render.
WAREHOUSE = Asset("duckdb://warehouse/ghflow")
VERDICTS = Asset("postgres://platform-db:5432/ghflow_ops/public/quality_verdicts")


def ghflow(*args: str, also_ok: tuple[int, ...] = ()) -> int:
    """Run one ghflow command in the project venv; its output goes to the task log."""
    done = subprocess.run([GHFLOW_PYTHON, "-m", *args], capture_output=True, text=True)
    print(done.stdout, end="")
    print(done.stderr, end="")
    if done.returncode not in (PASSED, *also_ok):
        raise RuntimeError(f"{' '.join(args)} exited {done.returncode}")
    return done.returncode


@dag(
    schedule=[WAREHOUSE],
    start_date=pendulum.datetime(2026, 8, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ghflow", "batch", "quality", "M1"],
    doc_md=__doc__,
)
def gharchive_quality() -> None:
    @task(pool="duckdb", outlets=[VERDICTS])  # reads the warehouse: never while dbt_build writes it
    def judge(run_id: str) -> None:
        ghflow("ghflow.commands.quality", "judge", "--run-id", run_id)

    @task
    def etag_check() -> None:
        ghflow("ghflow.commands.etag", "check", "--days", "14")

    @task
    def reingest() -> bool:
        """True if any hour was re-ingested (exit 0); exit 3 means there was nothing to do."""
        return ghflow("ghflow.commands.etag", "process", also_ok=(NO_VERDICT,)) == PASSED

    @task.short_circuit
    def anything_reingested(reingested: bool) -> bool:
        return reingested

    rebuild = TriggerDagRunOperator(
        task_id="rebuild_silver",
        trigger_dag_id="dbt_build",
        conf={"full_refresh": True},
    )

    count = reingest()
    judge(run_id="{{ run_id }}") >> etag_check() >> count
    anything_reingested(count) >> rebuild


gharchive_quality()

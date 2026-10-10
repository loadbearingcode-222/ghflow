"""reconcile: every hour, batch against stream by event id over the whole window (M2-S3, ADR-0007).

Green when the stream's ids for an hour equal bronze's; red (named ids) when one is missing or
extra; pending while the stream has not closed the hour. Results: ghflow_ops.reconcile_results.
A red hour fails the task, loudly.
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

GHFLOW_PYTHON = os.environ.get("GHFLOW_PYTHON", "/opt/ghflow-venv/bin/python")
#: The newest hour that can be closed: the one before the data interval's end.
LAST_HOUR = "{{ (data_interval_end - macros.timedelta(hours=2)).strftime('%Y-%m-%d') }}-{{ (data_interval_end - macros.timedelta(hours=2)).hour }}"


@dag(
    schedule=CronDataIntervalTimetable("45 * * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 8, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ghflow", "stream", "M2"],
    doc_md=__doc__,
)
def reconcile() -> None:
    @task(execution_timeout=timedelta(hours=1), retries=0)
    def batch_vs_stream(last_hour: str) -> None:
        done = subprocess.run(
            [GHFLOW_PYTHON, "-m", "ghflow.commands.reconcile", "run", "--to", last_hour],
            capture_output=True,
            text=True,
        )
        print(done.stdout, end="")
        print(done.stderr, end="")
        if done.returncode != 0:
            raise RuntimeError(f"reconcile exited {done.returncode}: see the red hours above")

    batch_vs_stream(last_hour=LAST_HOUR)


reconcile()

"""raw_retention: delete raw .gz files older than 14 days, once a day (M1-S2, ADR-0002).

"Older" is measured from the end of the day this run covers to each file's own hour, so a
re-run of the same day deletes the same set: the data interval is the clock. Bronze is never
listed, so it cannot be touched. Trigger with conf {"dry_run": true} to see the list first.
"""

from __future__ import annotations

import os
import subprocess

import pendulum
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

GHFLOW_PYTHON = os.environ.get("GHFLOW_PYTHON", "/opt/ghflow-venv/bin/python")
AS_OF = "{{ data_interval_end.isoformat() }}"
DRY_RUN = "{{ 'true' if (dag_run.conf or {}).get('dry_run') else 'false' }}"


@dag(
    schedule=CronDataIntervalTimetable(
        "0 1 * * *", timezone="UTC"
    ),  # daily, after midnight's hours land
    start_date=pendulum.datetime(2026, 10, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ghflow", "batch", "M1"],
    doc_md=__doc__,
)
def raw_retention() -> None:
    @task
    def delete_expired_raw(as_of: str, dry_run: str) -> None:
        args = [GHFLOW_PYTHON, "-m", "ghflow.commands.retention", "--as-of", as_of]
        if dry_run == "true":
            args.append("--dry-run")
        done = subprocess.run(args, capture_output=True, text=True)
        print(done.stdout, end="")
        print(done.stderr, end="")
        if done.returncode != 0:
            raise RuntimeError(f"retention exited {done.returncode}")

    delete_expired_raw(as_of=AS_OF, dry_run=DRY_RUN)


raw_retention()

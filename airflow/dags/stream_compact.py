"""stream_compact: every 15 minutes, compact each closed hour of the landed stream (M2-S2, ADR-0006).

An hour is closed when the lander has committed past its END_OF_HOUR marker on every partition.
Its small (partition, hour) files become one file of distinct events (deduplicated by event id,
checked content-equal to its inputs), and the small files are then deleted.
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

GHFLOW_PYTHON = os.environ.get("GHFLOW_PYTHON", "/opt/ghflow-venv/bin/python")


@dag(
    schedule=CronDataIntervalTimetable("*/15 * * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 8, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ghflow", "stream", "M2"],
    doc_md=__doc__,
)
def stream_compact() -> None:
    @task(execution_timeout=timedelta(hours=2))
    def compact_closed_hours() -> None:
        done = subprocess.run(
            [GHFLOW_PYTHON, "-m", "ghflow.commands.lander", "compact"],
            capture_output=True,
            text=True,
        )
        print(done.stdout, end="")
        print(done.stderr, end="")
        if done.returncode != 0:
            raise RuntimeError(f"lander compact exited {done.returncode}")

    compact_closed_hours()


stream_compact()

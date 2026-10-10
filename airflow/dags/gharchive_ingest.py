"""gharchive_ingest: one GH Archive hour into bronze per data interval (M1-S1).

The data interval IS the watermark: the run for [14:00, 15:00) ingests the file named
`<date>-14`, so there is no stored "last ingested" state to drift. Re-running an interval
rewrites identical bytes and appends an audit row.

Thin by design: Airflow's own interpreter never imports ghflow (it lives in an isolated venv,
docker/airflow). Each task runs `ghflow.commands.ingest` in that venv and maps its exit code
(ghflow.exitcodes) onto Airflow's vocabulary.

- `wait_for_publication` pokes in reschedule mode, so a waiting hour holds no worker slot and
  can never deadlock `max_active_runs` (wikiflow's pattern).
- `ingest` fails WITHOUT retry on a refusal (exit 1: a structure change is not transient) and
  retries anything else.
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.timetables.interval import CronDataIntervalTimetable

GHFLOW_PYTHON = os.environ.get("GHFLOW_PYTHON", "/opt/ghflow-venv/bin/python")
HOUR_KEY = "{{ data_interval_start.strftime('%Y-%m-%d') }}-{{ data_interval_start.hour }}"

# Exit codes, as ghflow.exitcodes defines them (this interpreter cannot import ghflow).
PASSED, REFUSED, NO_VERDICT = 0, 1, 3


def ghflow(*args: str) -> int:
    """Run one ghflow command in the project venv; its output goes to the task log."""
    done = subprocess.run([GHFLOW_PYTHON, "-m", *args], capture_output=True, text=True)
    print(done.stdout, end="")
    print(done.stderr, end="")
    return done.returncode


@dag(
    # Explicit: Airflow 3 reads a bare "@hourly" as a zero-length interval AT the trigger time,
    # which would render the hour still in progress (gotcha #43). This gives [H, H+1) at H+1.
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 8, 3, tz="UTC"),
    # True: hours missed while the stack was down are run when it comes back (gotcha #48). The
    # window before the first scheduled run was backfilled explicitly in M1-S2 (park/resume/watch).
    catchup=True,
    max_active_runs=4,  # = settings.CONCURRENCY: hours in flight against the archive
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ghflow", "batch", "M1"],
    doc_md=__doc__,
)
def gharchive_ingest() -> None:
    @task.sensor(poke_interval=120, timeout=6 * 3600, mode="reschedule", retries=0)
    def wait_for_publication(hour: str) -> bool:
        """True once the archive serves the hour (HTTP 200); 404 and failed requests wait."""
        return ghflow("ghflow.commands.ingest", "probe", hour) == PASSED

    @task(execution_timeout=timedelta(minutes=30))
    def ingest(hour: str) -> None:
        code = ghflow("ghflow.commands.ingest", "run", hour)
        if code == REFUSED:
            raise AirflowFailException(f"{hour} refused by the contract (see the audit row)")
        if code != PASSED:
            raise RuntimeError(f"{hour}: ingest exited {code}")

    # Call arguments are templated (op_kwargs); default parameter values would not be.
    wait_for_publication(hour=HOUR_KEY) >> ingest(hour=HOUR_KEY)


gharchive_ingest()

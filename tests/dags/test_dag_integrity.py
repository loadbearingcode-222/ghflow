"""DAG integrity, run INSIDE the Airflow image (CI's dag-integrity job, `.\\make.ps1 dags`).

Positive artifacts, not absent errors: the image runs the pinned Airflow, the project venv
imports ghflow, and every DAG file parses into exactly one DAG; then each DAG's shape.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import airflow
from airflow.models.dagbag import DagBag

DAGS = Path(os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags"))
PINNED_AIRFLOW = "3.3.2"  # ADR-0001; docker/airflow/Dockerfile


def test_the_image_runs_the_pinned_airflow() -> None:
    assert airflow.__version__ == PINNED_AIRFLOW


def test_every_dag_file_parses_into_exactly_one_dag() -> None:
    bag = DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}, bag.import_errors
    files = sorted(p.name for p in DAGS.glob("*.py"))
    assert len(bag.dags) == len(files), (sorted(bag.dags), files)


def test_the_project_venv_imports_ghflow() -> None:
    python = os.environ.get("GHFLOW_PYTHON", "/opt/ghflow-venv/bin/python")
    result = subprocess.run(
        [
            python,
            "-c",
            "import ghflow.archive, ghflow.contract, ghflow.bronze, ghflow.storage, ghflow.audit, ghflow.commands.ingest; print('ghflow ok')",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "ghflow ok"


def test_gharchive_ingest_waits_then_ingests_one_interval() -> None:
    dag = DagBag(dag_folder=str(DAGS)).dags["gharchive_ingest"]
    sensor, ingest = dag.get_task("wait_for_publication"), dag.get_task("ingest")
    assert [t.task_id for t in sensor.downstream_list] == ["ingest"]
    assert sensor.mode == "reschedule", "a waiting hour must not hold a worker slot"
    assert dag.catchup is True, "downtime must not lose hours (gotcha #48)"
    assert dag.max_active_runs == 4
    # The interval is the closed hour [H, H+1), triggered at H+1 — never a zero-length point.
    from airflow.timetables.interval import CronDataIntervalTimetable

    assert isinstance(dag.timetable, CronDataIntervalTimetable)
    for task in (sensor, ingest):
        assert "data_interval_start" in str(task.op_kwargs["hour"]), "the interval is the watermark"


def test_raw_retention_runs_daily_on_its_own_interval() -> None:
    from airflow.timetables.interval import CronDataIntervalTimetable

    dag = DagBag(dag_folder=str(DAGS)).dags["raw_retention"]
    assert isinstance(dag.timetable, CronDataIntervalTimetable) and dag.max_active_runs == 1
    task = dag.get_task("delete_expired_raw")
    assert "data_interval_end" in str(task.op_kwargs["as_of"]), "the interval is the clock"


def test_every_dag_starts_no_later_than_the_window() -> None:
    """A run before start_date gets no tasks yet reports success (gotcha #44)."""
    from datetime import UTC, datetime

    window_start = datetime(2026, 8, 3, tzinfo=UTC)
    for dag_id, dag in DagBag(dag_folder=str(DAGS)).dags.items():
        assert dag.start_date <= window_start, f"{dag_id} starts after the window"


def test_dbt_build_is_the_one_duckdb_writer() -> None:
    dag = DagBag(dag_folder=str(DAGS)).dags["dbt_build"]
    assert {t.pool for t in dag.tasks} == {"duckdb"}, "DuckDB has exactly one writer (ADR-0003)"
    assert "compact" in dag.get_task("build").downstream_task_ids, "compact after every build (#26)"
    assert dag.max_active_runs == 1


def test_gharchive_quality_judges_then_rechecks_then_reingests() -> None:
    from itertools import pairwise

    dag = DagBag(dag_folder=str(DAGS)).dags["gharchive_quality"]
    order = ["judge", "etag_check", "reingest", "anything_reingested", "rebuild_silver"]
    assert sorted(t.task_id for t in dag.tasks) == sorted(order)
    for upstream, downstream in pairwise(order):
        assert downstream in dag.get_task(upstream).downstream_task_ids, (upstream, downstream)
    assert dag.get_task("judge").pool == "duckdb", "the warehouse has one writer; read outside it"
    assert dag.get_task("rebuild_silver").conf == {"full_refresh": True}


def _schedule_uris(dag: object) -> set[str]:
    condition = dag.timetable.asset_condition  # type: ignore[attr-defined]
    return {asset.uri for asset in condition.objects}


def test_the_daily_chain_runs_on_assets_build_judge_marts_dashboard() -> None:
    """M5-S3: a day reaches the dashboard only after its hours are judged."""
    dags = DagBag(dag_folder=str(DAGS)).dags
    uris = {
        t.task_id: {o.uri for o in t.outlets} for d in dags.values() for t in d.tasks if t.outlets
    }
    assert uris["compact"] == {"duckdb://warehouse/ghflow"}
    assert _schedule_uris(dags["gharchive_quality"]) == {"duckdb://warehouse/ghflow"}
    assert uris["judge"] == {"postgres://platform-db:5432/ghflow_ops/public/quality_verdicts"}
    assert _schedule_uris(dags["marts_publish"]) == {
        "postgres://platform-db:5432/ghflow_ops/public/quality_verdicts"
    }
    assert uris["export"] == {"s3://marts/latest/manifest.json"}
    assert _schedule_uris(dags["dashboard_render"]) == {"s3://marts/latest/manifest.json"}
    publish = dags["marts_publish"]
    assert [t.task_id for t in publish.topological_sort()] == ["refresh", "twin", "export"]
    assert {t.pool for t in publish.tasks} == {"duckdb"}, "one writer (ADR-0003)"

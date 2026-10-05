"""DAG integrity, run INSIDE the Airflow image (CI's dag-integrity job, `.\\make.ps1 dags`).

Positive artifacts, not absent errors: the image runs the pinned Airflow, the project venv
imports ghflow, and every DAG file parses into exactly one DAG. Until M1-S1 the folder is empty,
and the count must say so (zero files, zero DAGs) rather than pass vacuously on an import error.
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
        [python, "-c", "import ghflow.archive, ghflow.contract; print('ghflow ok')"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "ghflow ok"

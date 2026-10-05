"""Every CI workflow parses, and every job in it has somewhere to run and something to do.

A workflow file that does not parse is the quietest CI failure there is: GitHub runs no jobs, the
PR shows "no checks reported", and that reads like "still queued" rather than "broken" (gotcha
#42). This parses the files the same way GitHub must before anything can run.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOWS = sorted((Path(__file__).resolve().parents[2] / ".github" / "workflows").glob("*.yml"))


def test_there_is_at_least_one_workflow() -> None:
    assert WORKFLOWS, "no workflow files found: CI would run nothing"


@pytest.mark.parametrize("path", WORKFLOWS, ids=[p.name for p in WORKFLOWS])
def test_the_workflow_parses_and_every_job_has_steps(path: Path) -> None:
    workflow: dict[Any, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(workflow, dict), f"{path.name} is not a mapping"
    jobs = workflow.get("jobs")
    assert isinstance(jobs, dict) and jobs, f"{path.name} defines no jobs"
    for name, job in jobs.items():
        assert job.get("runs-on"), f"{path.name}: job {name} has no runs-on"
        steps = job.get("steps")
        assert isinstance(steps, list) and steps, f"{path.name}: job {name} has no steps"
        for step in steps:
            assert ("run" in step) != ("uses" in step), (
                f"{path.name}: job {name} has a step with neither or both of run/uses"
            )


def test_a_planted_broken_block_is_caught() -> None:
    """Red team: the exact break M0-S4 shipped — a newline injected into a `run: |` block."""
    broken = "jobs:\n  x:\n    runs-on: ubuntu-latest\n    steps:\n      - run: |\n          echo \"a | tr '\n' ' '\"\n"
    with pytest.raises(yaml.YAMLError):
        yaml.safe_load(broken)

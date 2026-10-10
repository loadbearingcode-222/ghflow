"""M1-S4's order law, checked from git: the bars, then the prediction, then the run.

A prediction committed after the run would be a description; bars committed after the
prediction would be fitted to it. Fails closed on a shallow clone (CI fetches full history).
"""

from __future__ import annotations

import subprocess
from itertools import pairwise
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNS = "automation/runs/m1-s4"
ORDER = ("bars.json", "prediction.json", "verdicts.json")


def git(*args: str) -> str:
    done = subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=False
    )
    return done.stdout.strip()


def first_commit(path: str) -> str:
    """The commit that added `path` (oldest first)."""
    added = git("log", "--reverse", "--diff-filter=A", "--format=%H", "--", path).splitlines()
    assert added, f"{path} is not committed"
    return added[0]


def before(a: str, b: str) -> bool:
    """Commit a is a strict ancestor of commit b."""
    ok = subprocess.run(["git", "merge-base", "--is-ancestor", a, b], cwd=REPO_ROOT, check=False)
    return ok.returncode == 0 and a != b


def test_the_history_is_deep_enough_to_judge() -> None:
    assert git("rev-parse", "--is-shallow-repository") == "false", (
        "fetch full history (fetch-depth: 0)"
    )


def test_bars_then_prediction_then_run() -> None:
    named = [(name, first_commit(f"{RUNS}/{name}")) for name in ORDER]
    for (name_a, a), (name_b, b) in pairwise(named):
        assert before(a, b), f"{name_a} ({a[:7]}) must be committed before {name_b} ({b[:7]})"

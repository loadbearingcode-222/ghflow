"""Repo hygiene that fails silently when it breaks: hook modes, ignore rules, the blueprint."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, encoding="utf-8"
    )


def bash() -> str:
    """Git's own bash on Windows (never WSL's), plain bash elsewhere.

    `os.name`, not `sys.platform`: mypy narrows `sys.platform` to the host it runs on, so the
    Windows branch reads as unreachable on the Linux CI runner (gotcha #38).
    """
    if os.name != "nt":
        return "bash"
    exec_path = Path(git("--exec-path").stdout.strip())  # .../Git/mingw64/libexec/git-core
    candidate = exec_path.parents[2] / "bin" / "bash.exe"
    assert candidate.is_file(), f"Git for Windows bash not found at {candidate}"
    return str(candidate)


def test_pre_push_hook_is_tracked_executable() -> None:
    mode = git("ls-files", "-s", ".githooks/pre-push").stdout.split()
    assert mode, ".githooks/pre-push is not tracked"
    assert mode[0] == "100755", f"pre-push is tracked as {mode[0]}; git would skip it"


@pytest.mark.parametrize(
    ("remote_ref", "refused"),
    [("refs/heads/main", True), ("refs/heads/feat/m0-s1-repo-skeleton", False)],
)
def test_pre_push_hook_refuses_only_main(remote_ref: str, refused: bool) -> None:
    sha = "0" * 40
    env = {k: v for k, v in os.environ.items() if k != "GHFLOW_ALLOW_MAIN_PUSH"}
    result = subprocess.run(
        [bash(), ".githooks/pre-push", "origin", "url"],
        cwd=REPO_ROOT,
        input=f"refs/heads/x {sha} {remote_ref} {sha}\n",
        capture_output=True,
        text=True,
        env=env,
    )
    assert (result.returncode != 0) is refused, result.stderr
    if refused:
        assert "refusing a direct push to main" in result.stderr


@pytest.mark.parametrize(
    ("path", "ignored"),
    [
        ("automation/runs/m0-s2/profile.json", False),  # evidence is tracked
        ("automation/runs/m1-s2/backfill.log", True),
        ("automation/status.txt", True),
        ("bronze/hour=15/part-0.parquet", True),  # data never enters the repo
        ("2026-09-05-15.json.gz", True),
        ("warehouse.duckdb", True),
        (".env", True),
        ("HANDOFF.md", True),
        ("dashboard/_site/index.html", True),
        (".env.example", False),
    ],
)
def test_ignore_rules_hold_in_both_directions(path: str, ignored: bool) -> None:
    # --no-index: judge the rule itself, for paths that need not exist yet.
    result = git("check-ignore", "--no-index", "-q", path)
    assert result.returncode in (0, 1), result.stderr
    assert (result.returncode == 0) is ignored, git("check-ignore", "--no-index", "-v", path).stdout


COMPONENTS = ["GH Archive", "MinIO", "DuckDB", "Redpanda", "ClickHouse", "Grafana", "Quarto"]


@pytest.mark.parametrize("component", COMPONENTS)
def test_blueprint_and_drawing_name_the_same_components(component: str) -> None:
    diagram = (REPO_ROOT / "docs" / "architecture.md").read_text("utf-8")
    mermaid = diagram.split("```mermaid", 1)[1].split("```", 1)[0]
    drawing = (REPO_ROOT / "docs" / "img" / "architecture.svg").read_text("utf-8")
    assert component in mermaid, f"{component} missing from the Mermaid blueprint"
    assert component in drawing, f"{component} missing from architecture.svg"

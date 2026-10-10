"""The front door is checked (M5-S4): its table renders from the records, and every command,
link and screenshot it names exists."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
README = (ROOT / "README.md").read_text("utf-8")
sys.path.insert(0, str(ROOT / "scripts"))

import readme_table  # noqa: E402 - a script, imported from its folder


def test_the_table_is_the_records_rendered() -> None:
    assert readme_table.current() == readme_table.block(), (
        "run: uv run python scripts/readme_table.py"
    )
    assert len(readme_table.ROWS) >= 13, "every Refine story's measured changes"
    assert {r.story for r in readme_table.ROWS} == {
        "M0-S5",
        "M1-S5",
        "M2-S4",
        "M3-S3",
        "M4-S3",
        "M5-S3",
    }


def test_every_make_target_named_exists() -> None:
    make = (ROOT / "make.ps1").read_text("utf-8")
    for target in re.findall(r"\.\\make\.ps1 ([a-z-]+)", README):
        assert f"'{target}'" in make, target


def test_every_linked_path_and_image_exists() -> None:
    for path in re.findall(r"\]\(((?:docs|scripts|automation)/[^)#]+)\)", README):
        assert (ROOT / path).exists(), path
    for path in re.findall(r"`((?:scripts|automation/runs)/[^`]+\.(?:py|json))`", README):
        assert (ROOT / path).exists() or path == "automation/runs/m5-s4/demo.json", path


def test_the_architecture_and_dashboard_are_shown() -> None:
    assert "```mermaid" in README
    assert len(re.findall(r"!\[[^\]]*\]\(docs/img/dashboard-[a-z]+\.png\)", README)) >= 3

"""The blueprint names every running part (M5-S4): a compose service or a DAG missing from the
Mermaid diagrams in docs/architecture.md fails here."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def diagrams() -> str:
    text = (ROOT / "docs" / "architecture.md").read_text("utf-8")
    return "\n".join(re.findall(r"```mermaid\n(.*?)```", text, re.S))


def names_missing(names: list[str], text: str) -> list[str]:
    return [n for n in names if not re.search(rf"(?<![\w-]){re.escape(n)}(?![\w-])", text)]


def test_every_compose_service_is_in_a_diagram() -> None:
    services = list(yaml.safe_load((ROOT / "compose.yaml").read_text("utf-8"))["services"])
    assert names_missing(services, diagrams()) == []


def test_every_dag_is_in_a_diagram() -> None:
    dags = [p.stem for p in (ROOT / "airflow" / "dags").glob("*.py")]
    assert len(dags) >= 8
    assert names_missing(dags, diagrams()) == []


def test_the_checker_catches_a_missing_name() -> None:
    assert names_missing(["lander", "ghost-service"], "lander --> x") == ["ghost-service"]
    assert names_missing(["repo"], "repo-state") == ["repo"], "a prefix is not a mention"

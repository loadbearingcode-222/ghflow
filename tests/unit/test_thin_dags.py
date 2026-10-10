"""DAG files stay thin (M1-S5): orchestration only; parsing, Parquet and SQL live in src/ghflow.

Airflow's interpreter never imports ghflow (an isolated venv, ADR-0002), so a DAG that parsed or
queried data would be a second implementation nobody tests. Checked by AST over every DAG file,
with a red team on planted source.
"""

from __future__ import annotations

import ast
from pathlib import Path

DAGS = sorted((Path(__file__).resolve().parents[2] / "airflow" / "dags").glob("*.py"))

#: Data handling has a home in src/ghflow; a DAG importing any of these is doing its work.
DATA_MODULES = {
    "json", "gzip", "csv", "pyarrow", "duckdb", "psycopg", "psycopg2", "sqlalchemy", "pandas",
    "polars", "orjson", "ghflow", "urllib", "requests", "boto3",
}  # fmt: skip
SQL_WORDS = ("select ", "insert into", "create table", "delete from", "update ", "copy ")


def docstrings(tree: ast.Module) -> set[int]:
    """ids of the module's and every function's docstring node: prose, not code."""
    owners = [
        tree,
        *(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)),
    ]
    return {
        id(owner.body[0].value)
        for owner in owners
        if owner.body
        and isinstance(owner.body[0], ast.Expr)
        and isinstance(owner.body[0].value, ast.Constant)
    }


def sql_like(node: ast.AST, prose: set[int]) -> bool:
    """A string constant, not a docstring, that reads like SQL."""
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in prose
        and any(word in node.value.lower() for word in SQL_WORDS)
    )


def offences(source: str) -> list[str]:
    tree = ast.parse(source)
    prose = docstrings(tree)
    found = []
    for node in ast.walk(tree):
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            names = [node.module]
        elif sql_like(node, prose):
            found.append(
                f"line {getattr(node, 'lineno', 0)}: SQL-like string {getattr(node, 'value', '')[:40]!r}"
            )
        found += [
            f"line {getattr(node, 'lineno', 0)}: imports {n}"
            for n in names
            if n.split(".")[0] in DATA_MODULES
        ]
    return found


def test_there_are_dags_to_check() -> None:
    assert len(DAGS) >= 4


def test_every_dag_is_thin() -> None:
    offenders = {p.name: f for p in DAGS if (f := offences(p.read_text(encoding="utf-8")))}
    assert offenders == {}, f"move this into src/ghflow and call it as a command: {offenders}"


def test_the_check_catches_planted_data_handling() -> None:
    planted = (
        "import json\nimport pyarrow.parquet as pq\nfrom ghflow.contract import apply\n"
        "QUERY = 'select count(*) from ingest_audit'\n"
    )
    assert len(offences(planted)) == 4

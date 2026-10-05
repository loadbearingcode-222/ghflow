"""The contract against its 16 declared hazard rows, plus the laws that keep it the only cast."""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

import pytest

from factories import line, push
from ghflow import fields
from ghflow.commands import contract as contract_cli
from ghflow.contract import ROW_RULES, Refusal, apply
from ghflow.exitcodes import ExitCode, guarded
from ghflow.fields import COLUMNS
from ghflow.hours import Hour

REPO_ROOT = Path(__file__).resolve().parents[2]
HAZARDS: list[dict[str, Any]] = json.loads(
    (REPO_ROOT / "tests" / "fixtures" / "hazard_rows.json").read_text("utf-8")
)


def clean_line(hazard: dict[str, Any]) -> str:
    """A healthy neighbour for the hazard row, with its own id unless it must collide."""
    event = json.loads(HAZARDS[0]["line"])
    if hazard["hour"] != HAZARDS[0]["hour"]:
        event = json.loads(HAZARDS[1]["line"])
    if not hazard["expect"].get("after_clean_copy"):
        event["id"] = "40000000999"
    return json.dumps(event)


def test_the_hazard_set_is_declared_once_and_complete() -> None:
    assert len(HAZARDS) == 16
    assert len({h["name"] for h in HAZARDS}) == 16
    assert all(h["hazard"] for h in HAZARDS), "every row names its hazard"
    outcomes = {h["expect"]["outcome"] for h in HAZARDS}
    assert outcomes == {"accepted", "rejected", "refused"}


@pytest.mark.parametrize("hazard", HAZARDS, ids=[h["name"] for h in HAZARDS])
def test_each_hazard_row_produces_its_declared_outcome(hazard: dict[str, Any]) -> None:
    hour = Hour.parse(hazard["hour"])
    lines = [clean_line(hazard), hazard["line"]]
    expect = hazard["expect"]

    if expect["outcome"] == "refused":
        with pytest.raises(Refusal) as refusal:
            apply(lines, hour)
        assert refusal.value.code == expect["code"]
        return

    result = apply(lines, hour)
    assert result.lines_in == len(result.rows) + len(result.rejected) == 2
    if expect["outcome"] == "rejected":
        assert [r["rule"] for r in result.rejected] == [expect["rule"]]
        assert result.rules == {expect["rule"]: 1}
        return

    assert result.rejected == []
    row = result.rows[-1]
    assert set(row) == set(COLUMNS)
    for column, value in expect.get("row", {}).items():
        assert row[column] == value, column
    if "era_violation" in expect:
        assert result.era_violations == {expect["era_violation"]: 1}
    if "unknown_type" in expect:
        assert result.unknown_types == {expect["unknown_type"]: 1}


def test_every_row_rule_is_exercised_by_a_hazard_or_has_a_reason() -> None:
    exercised = {h["expect"].get("rule") for h in HAZARDS}
    unexercised = set(ROW_RULES) - exercised
    assert unexercised == {"actor_malformed"}, unexercised  # the twin of repo_malformed


def push_hour(rows: int, without_ref: int) -> list[bytes]:
    lines = []
    for n in range(rows):
        event = push(50_000_000_000 + n)
        if n < without_ref:
            del event["payload"]["ref"]
        lines.append(line(event))
    return lines


def test_a_field_vanishing_from_every_row_refuses_the_hour() -> None:
    with pytest.raises(Refusal) as refusal:
        apply(push_hour(rows=20, without_ref=20), Hour.parse("2026-10-02-15"))
    assert refusal.value.code == "STRUCTURE-FIELD-VANISHED"
    assert "PushEvent.ref" in refusal.value.detail


@pytest.mark.parametrize(("rows", "without_ref"), [(19, 19), (20, 19)])
def test_a_field_missing_from_some_rows_is_counted_not_refused(rows: int, without_ref: int) -> None:
    result = apply(push_hour(rows, without_ref), Hour.parse("2026-10-02-15"))
    assert result.missing_in_era["PushEvent.ref"] == without_ref
    assert len(result.rows) == rows


def test_the_cli_refuses_with_exit_1_and_writes_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    hazard = next(h for h in HAZARDS if h["name"] == "unknown_top_level_key")
    source = tmp_path / "hour.json"
    source.write_text(clean_line(hazard) + "\n" + hazard["line"] + "\n", encoding="utf-8")
    code = contract_cli.main([hazard["hour"], "--file", str(source)])
    out = capsys.readouterr()
    assert code == ExitCode.REFUSED
    assert int(ExitCode.REFUSED) == 1  # the vocabulary, not a magic number
    assert out.out == ""
    assert "STRUCTURE-UNKNOWN-KEY" in out.err
    assert sorted(p.name for p in tmp_path.iterdir()) == ["hour.json"]


def test_an_unenumerated_exception_exits_4_never_1() -> None:
    def boom() -> int:
        raise KeyError("nobody enumerated this")

    assert guarded(boom) == ExitCode.CRASHED
    assert int(ExitCode.CRASHED) == 4


# ------------------------------------------------------------------ the one-cast law, by AST

THE_CAST = REPO_ROOT / "src" / "ghflow" / "contract.py"
CASTS = {"int", "float", "str", "bool", "Decimal", "strptime", "fromisoformat", "fromtimestamp"}


def source_field_access(node: ast.AST) -> str | None:
    """`x["created_at"]` or `x.get("created_at", ...)` with a source key, else None."""
    if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
        key = node.slice.value
    elif (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    ):
        key = node.args[0].value
    else:
        return None
    return key if isinstance(key, str) and key in fields.SOURCE_KEYS else None


def casts_of_source_fields(tree: ast.AST) -> list[str]:
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = node.func
        name = (
            func.id
            if isinstance(func, ast.Name)
            else func.attr
            if isinstance(func, ast.Attribute)
            else ""
        )
        if name in CASTS and (key := source_field_access(node.args[0])):
            found.append(f"line {node.lineno}: {name}(…{key!r}…)")
    return found


def test_only_the_contract_casts_a_source_field() -> None:
    offenders = {}
    for path in sorted((REPO_ROOT / "src" / "ghflow").rglob("*.py")):
        if path == THE_CAST:  # by path, not name: commands/contract.py gets no exemption
            continue
        if found := casts_of_source_fields(ast.parse(path.read_text("utf-8"))):
            offenders[str(path.relative_to(REPO_ROOT))] = found
    assert offenders == {}, f"only contract.py may cast a source field: {offenders}"


def test_the_ast_check_catches_a_planted_second_cast() -> None:
    """Red team on synthetic source: never touches a real module."""
    planted = 'def f(event):\n    return int(event["id"]) + int(event.get("push_id", 0))\n'
    assert len(casts_of_source_fields(ast.parse(planted))) == 2
    harmless = 'def f(response):\n    return int(response.headers.get("Content-Length"))\n'
    assert casts_of_source_fields(ast.parse(harmless)) == []

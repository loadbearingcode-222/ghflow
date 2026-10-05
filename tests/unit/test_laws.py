"""The one-home laws and the readability ratchet, checked by AST over the real source.

Each law has a red team on planted, synthetic source, so a check that silently passes
everything would fail here. Laws (docs/ITERATION.md, "How readability and cohesion are measured"):

- one JSON-line reader: only `ghflow.events` parses JSON (commands may read their own records);
- every CLI exits through `guarded`, so an unenumerated exception exits 4, never 1;
- one home per knob: a module-level numeric constant lives in `settings.py`, nowhere else;
- the pure core does no I/O: no network, files, processes or printing;
- the ratchet: complexity, size and argument floors only tighten.
"""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "src" / "ghflow"


def modules() -> list[Path]:
    return sorted(SRC.rglob("*.py"))


def rel(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def call_name(node: ast.Call) -> str:
    """`json.loads(...)` -> "json.loads", `open(...)` -> "open", else ""."""
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        return f"{func.value.id}.{func.attr}"
    return ""


def calls(module: ast.AST) -> list[tuple[int, str]]:
    return [(n.lineno, call_name(n)) for n in ast.walk(module) if isinstance(n, ast.Call)]


# ------------------------------------------------------------------ one JSON-line reader

PARSERS = {"json.loads", "json.load", "orjson.loads"}


def parses(module: ast.AST) -> list[str]:
    return [f"line {n}: {name}" for n, name in calls(module) if name in PARSERS]


def test_only_the_reader_parses_json() -> None:
    allowed = {SRC / "events.py"} | set((SRC / "commands").glob("*.py"))
    offenders = {rel(p): found for p in modules() if p not in allowed if (found := parses(tree(p)))}
    assert offenders == {}, f"parse lines through ghflow.events: {offenders}"


def test_the_reader_check_catches_a_planted_second_parser() -> None:
    planted = "import json\ndef f(lines):\n    return [json.loads(x) for x in lines]\n"
    assert parses(ast.parse(planted)) == ["line 3: json.loads"]


# ------------------------------------------------------------------ every CLI is guarded


def unguarded_mains(module: ast.Module) -> list[str]:
    found = []
    for node in module.body:
        is_main = isinstance(node, ast.FunctionDef) and node.name == "main"
        if is_main and "guarded" not in {name for _, name in calls(node)}:
            found.append(f"line {node.lineno}: main does not call guarded()")
    return found


def is_entry_point(module: ast.Module) -> bool:
    return any(
        isinstance(node, ast.If) and "__name__" in ast.unparse(node.test) for node in module.body
    )


def test_every_cli_lives_in_commands_and_exits_through_guarded() -> None:
    offenders = {}
    for path in modules():
        module = tree(path)
        found = unguarded_mains(module)
        if is_entry_point(module) and path.parent != SRC / "commands":
            found.append("an entry point outside ghflow.commands")
        if found:
            offenders[rel(path)] = found
    assert offenders == {}, offenders


def test_the_guard_check_catches_a_planted_bare_main() -> None:
    planted = "def main():\n    return run()\nif __name__ == '__main__':\n    main()\n"
    module = ast.parse(planted)
    assert unguarded_mains(module) and is_entry_point(module)


# ------------------------------------------------------------------ one home per knob


def knobs(module: ast.Module) -> list[str]:
    """Module-level UPPER_CASE names bound to a number: tunables, wherever they hide."""
    found = []
    for node in module.body:
        target, value = None, None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign):
            target, value = node.target, node.value
        if isinstance(value, ast.UnaryOp):
            value = value.operand
        numeric = isinstance(value, ast.Constant) and type(value.value) in (int, float)
        if isinstance(target, ast.Name) and target.id.isupper() and numeric:
            found.append(f"line {node.lineno}: {target.id}")
    return found


def test_every_knob_lives_in_settings() -> None:
    offenders = {rel(p): k for p in modules() if p.name != "settings.py" if (k := knobs(tree(p)))}
    assert offenders == {}, f"move these to ghflow.settings with their reason: {offenders}"


def test_the_knob_check_catches_a_planted_constant() -> None:
    planted = "RETRIES = 3\nPAUSE: float = -0.5\nNAME = 'x'\nlowercase = 1\n"
    assert knobs(ast.parse(planted)) == ["line 1: RETRIES", "line 2: PAUSE"]


# ------------------------------------------------------------------ the core does no I/O

CORE = ("settings", "hours", "events", "fields", "contract", "profiling")
IO_MODULES = {
    "gzip", "http", "io", "os", "pathlib", "requests", "shutil", "socket", "subprocess", "sys",
    "urllib", "ghflow.archive", "ghflow.commands",
}  # fmt: skip
IO_CALLS = {"open", "print", "input"}


def io_in(module: ast.Module) -> list[str]:
    found = []
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            # `from ghflow import archive` imports ghflow.archive as surely as the dotted form
            names = [node.module] + [f"{node.module}.{alias.name}" for alias in node.names]
        else:
            continue
        hits = [n for n in names if any(n == m or n.startswith(f"{m}.") for m in IO_MODULES)]
        if hits:
            found.append(f"line {node.lineno}: imports {hits[-1]}")
    found += [f"line {n}: calls {name}()" for n, name in calls(module) if name in IO_CALLS]
    return found


def test_the_pure_core_does_no_io() -> None:
    offenders = {name: f for name in CORE if (f := io_in(tree(SRC / f"{name}.py")))}
    assert offenders == {}, f"I/O belongs in the shell (archive, commands): {offenders}"


def test_the_io_check_catches_planted_io() -> None:
    planted = (
        "import urllib.request\nfrom ghflow.archive import head\nfrom ghflow import commands\n"
        "def f():\n    print(1)\n"
    )
    assert len(io_in(ast.parse(planted))) == 4


# ------------------------------------------------------------------ the ratchet

#: The floors. Lowering a value here (and in pyproject.toml) tightens the ratchet; raising one
#: loosens it and needs an ADR. pyproject.toml may be stricter than these, never looser.
FLOORS = {"max-complexity": 8, "max-args": 5, "max-branches": 10, "max-statements": 40}
MODULE_LINES = 300
RULES = {"C90", "PLR0912", "PLR0913", "PLR0915"}


def test_the_ratchet_rules_are_on_and_no_looser_than_the_floors() -> None:
    ruff = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text("utf-8"))["tool"]["ruff"]["lint"]
    assert set(ruff["select"]) >= RULES, "the ratchet's ruff rules must stay selected"
    assert not RULES & set(ruff.get("ignore", [])) and "per-file-ignores" not in ruff
    configured = ruff["mccabe"] | ruff["pylint"]
    looser = {k: configured[k] for k, floor in FLOORS.items() if configured[k] > floor}
    assert looser == {}, f"a floor was loosened without an ADR: {looser}"


@pytest.mark.parametrize("root", ["src", "scripts", ".github/scripts", "tests"])
def test_every_module_fits_the_size_budget(root: str) -> None:
    sizes = {
        rel(p): n
        for p in (REPO_ROOT / root).rglob("*.py")
        if (n := len(p.read_bytes().splitlines())) > MODULE_LINES
    }
    assert sizes == {}, f"over {MODULE_LINES} lines: split by responsibility: {sizes}"

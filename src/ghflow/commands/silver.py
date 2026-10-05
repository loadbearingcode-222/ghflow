"""Write, or check, the generated dbt silver models and rows test (ghflow.silver) under dbt/ghflow.

    python -m ghflow.commands.silver generate
    python -m ghflow.commands.silver check     # exit 1 and name the files that differ
    python -m ghflow.commands.silver redteam   # in the stack: plant an era violation in a copy

The models' column lists, types and era tests come from ghflow.fields, so a field's era is
declared once; `check` (also a unit test) refuses committed files that drifted from it.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from ghflow import silver
from ghflow.exitcodes import ExitCode, guarded

DBT_DIR = Path(__file__).resolve().parents[3] / "dbt" / "ghflow"
GENERATED_DIR = "models/silver"  # wholly generated: a file here that silver.files() lacks is stray


def stale(root: Path = DBT_DIR) -> list[str]:
    """Generated files that are missing or differ, plus stray files in the generated directory."""
    wanted = silver.files()
    differ = [
        name
        for name, text in wanted.items()
        if not (root / name).exists() or (root / name).read_bytes() != text.encode()
    ]
    present = {f"{GENERATED_DIR}/{p.name}" for p in (root / GENERATED_DIR).glob("*") if p.is_file()}
    return sorted(differ + sorted(present - set(wanted)))


def generate(root: Path = DBT_DIR) -> None:
    for stray in {p for p in (root / GENERATED_DIR).glob("*") if p.is_file()}:
        if f"{GENERATED_DIR}/{stray.name}" not in silver.files():
            stray.unlink()
    for name, text in silver.files().items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_bytes(text.encode())


PLANTED_TEST = "era__silver_push_events__push_size__until_2025_10_08T17"
NEIGHBOUR_TEST = "era__silver_push_events__push_distinct_size__until_2025_10_08T17"


def build_redteam_copy(prod: str, copy: str) -> None:
    """A disposable warehouse: one real day of PushEvents, plus one push_size in the wrong era."""
    import duckdb  # the dbt group's dependency; present wherever dbt runs

    Path(copy).unlink(missing_ok=True)
    with duckdb.connect(copy) as con:
        # The views name their catalog after the file (ghflow), so the attach must use that name.
        con.execute(f"attach '{prod}' as ghflow (read_only)")
        con.execute("create schema main_silver")
        con.execute(
            "create table main_silver.silver_push_events as select * from "
            "ghflow.main_silver.silver_push_events where event_date = date '2026-08-03'"
        )
        # GitHub removed PushEvent `size` at 2025-10-08 17:00; every row here is after that.
        con.execute(
            "update main_silver.silver_push_events set push_size = 7 "
            "where id = (select min(id) from main_silver.silver_push_events)"
        )


def redteam() -> int:
    prod = os.environ.get("GHFLOW_WAREHOUSE", "/opt/ghflow/warehouse/ghflow.duckdb")
    copy = os.environ.setdefault("GHFLOW_REDTEAM_WAREHOUSE", "/tmp/ghflow-redteam.duckdb")
    build_redteam_copy(prod, copy)
    dbt = str(Path(sys.executable).with_name("dbt"))
    project = str(DBT_DIR)
    args = [dbt, "test", "--project-dir", project, "--profiles-dir", project, "--target", "redteam"]
    subprocess.run([*args, "--select", PLANTED_TEST, NEIGHBOUR_TEST], capture_output=True)
    results = json.loads((DBT_DIR / "target" / "run_results.json").read_text(encoding="utf-8"))
    outcome = {
        r["unique_id"].split(".")[2]: (r["status"], r["failures"]) for r in results["results"]
    }
    Path(copy).unlink(missing_ok=True)
    print(json.dumps({"planted": "push_size = 7 on one 2026-08-03 PushEvent", "results": outcome}))
    caught = (
        outcome.get(PLANTED_TEST) == ("fail", 1) and outcome.get(NEIGHBOUR_TEST, ("",))[0] == "pass"
    )
    return ExitCode.PASSED if caught else ExitCode.REFUSED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["generate", "check", "redteam"])
    args = parser.parse_args(argv)

    def go() -> int:
        if args.command == "redteam":
            return redteam()
        if args.command == "generate":
            generate()
            print(f"wrote {len(silver.files())} files under {DBT_DIR}")
            return ExitCode.PASSED
        if differ := stale():
            print(f"stale generated silver files (run generate): {differ}", file=sys.stderr)
            return ExitCode.REFUSED
        return ExitCode.PASSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

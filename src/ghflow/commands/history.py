"""Prove the SCD2 dimensions do not depend on the order hours arrive in (M3-S1, ADR-0008).

    python -m ghflow.commands.history late-hour     # in the stack; a disposable warehouse only

Three days of real bronze are copied into a scratch directory and built into a disposable
warehouse (the dbt `redteam` target), three times:

1. **late**: every hour but one early hour, then that hour arrives after the later ones and the
   normal incremental build picks it up (it is inside the 2-day lookback);
2. **full**: a `--full-refresh` over the same files.

`dim_repo` and `dim_actor` after (1) must equal (2) row for row (symmetric EXCEPT ALL). A
control proves the hour mattered: the dimensions *before* it arrived differ from the full
rebuild. The production warehouse is never opened for writing; the scratch is removed.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from ghflow import bronze, storage
from ghflow.exitcodes import ExitCode, guarded
from ghflow.hours import Hour, hours_of_days

DBT_DIR = Path(__file__).resolve().parents[3] / "dbt" / "ghflow"
DIMENSIONS = ("dim_repo", "dim_actor")
SELECT = ["stg_gharchive__events", *DIMENSIONS]


def copy_bronze(store: storage.Store, hours: list[Hour], root: Path) -> list[Hour]:
    """Bronze files for the hours, laid out as in S3 (hive partitions); the ones that exist."""
    copied = []
    for hour in hours:
        key = bronze.bronze_path(hour)
        if not store.exists(key):
            continue
        target = root / key.split("/", 2)[2]  # drop "bronze/gharchive/"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(store.get(key))
        copied.append(hour)
    return copied


def dbt_build(root: Path, full_refresh: bool) -> None:
    dbt = str(Path(sys.executable).with_name("dbt"))
    project = str(DBT_DIR)
    variables = json.dumps({"bronze_events": f"{root}/*/*/events.parquet"})
    args = [
        dbt,
        "build",
        "--project-dir",
        project,
        "--profiles-dir",
        project,
        "--target",
        "redteam",
    ]
    args += ["--select", *SELECT, "--exclude", "silver_rows_equal_bronze_rows", "--vars", variables]
    if full_refresh:
        args.append("--full-refresh")
    done = subprocess.run(args, capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(f"dbt build exited {done.returncode}:\n{done.stdout[-4000:]}")


def snapshot(warehouse: str, snapshots: str, state: str) -> dict[str, Any]:
    """Copy the dimensions into the snapshot file under `state`; return their counts."""
    import duckdb  # the dbt group's dependency; present wherever dbt runs

    counts = {}
    with duckdb.connect(snapshots) as con:
        con.execute(f"attach '{warehouse}' as ghflow (read_only)")
        for dim in DIMENSIONS:
            con.execute(f"create table {state}_{dim} as select * from ghflow.main_gold.{dim}")
            counts[dim] = con.execute(f"select count(*) from {state}_{dim}").fetchone()[0]  # type: ignore[index]
    return counts


def differences(snapshots: str, left: str, right: str) -> dict[str, int]:
    """Rows in one state but not the other, both directions, per dimension (bag semantics)."""
    import duckdb

    with duckdb.connect(snapshots, read_only=True) as con:
        return {
            dim: con.execute(
                f"select (select count(*) from (select * from {left}_{dim} except all "
                f"select * from {right}_{dim})) + (select count(*) from (select * from "
                f"{right}_{dim} except all select * from {left}_{dim}))"
            ).fetchone()[0]  # type: ignore[index]
            for dim in DIMENSIONS
        }


def late_hour(first: date, days: int, late: Hour, scratch: Path) -> dict[str, Any]:
    shutil.rmtree(scratch, ignore_errors=True)
    bronze, warehouse = scratch / "bronze", str(scratch / "history.duckdb")
    snapshots = str(scratch / "snapshots.duckdb")
    os.environ["GHFLOW_REDTEAM_WAREHOUSE"] = warehouse
    store = storage.Store(storage.S3Config.from_env())
    hours = hours_of_days(first, first + timedelta(days=days - 1))
    copied = copy_bronze(store, [h for h in hours if h != late], bronze)
    dbt_build(bronze, full_refresh=True)
    counts = {"before": snapshot(warehouse, snapshots, "before")}
    copied += copy_bronze(store, [late], bronze)
    dbt_build(bronze, full_refresh=False)  # the late hour arrives; the normal incremental build
    counts["late"] = snapshot(warehouse, snapshots, "late")
    dbt_build(bronze, full_refresh=True)
    counts["full"] = snapshot(warehouse, snapshots, "full")
    result = {
        "window": f"{first} .. {first + timedelta(days=days - 1)}",
        "late_hour": late.key,
        "hours": len(copied),
        "rows": counts,
        "late_vs_full": differences(snapshots, "late", "full"),
        "control_before_vs_full": differences(snapshots, "before", "full"),
    }
    shutil.rmtree(scratch, ignore_errors=True)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["late-hour"])
    parser.add_argument("--first", type=date.fromisoformat, default=date(2026, 8, 4))
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--late", type=Hour.parse, default=Hour.parse("2026-08-04-10"))
    parser.add_argument("--scratch", type=Path, default=Path("/tmp/ghflow-history"))
    args = parser.parse_args(argv)

    def go() -> int:
        result = late_hour(args.first, args.days, args.late, args.scratch)
        print(json.dumps(result, indent=2))
        equal = not any(result["late_vs_full"].values())
        mattered = all(result["control_before_vs_full"].values())
        return ExitCode.PASSED if equal and mattered else ExitCode.REFUSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

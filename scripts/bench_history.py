"""History benchmarks (M3-S3): SCD2 build time and repo-state restore time, interleaved.

Both run inside one-off containers on disposable state; nothing in production is written.

    # SCD2: the airflow image, the warehouse volume read-only, this repo's gold files as "after"
    docker run --rm --memory 4g -v ghflow_warehouse:/wh:ro -v <repo>:/repo:ro \
        --entrypoint /opt/ghflow-venv/bin/python ghflow/airflow:dev /repo/scripts/bench_history.py scd2
    # restore: the app image on the compose network ("before" = full rebuild, "after" = tail)
    docker run --rm --network ghflow_default --memory 768m -e KAFKA_BOOTSTRAP=redpanda:9092 \
        -v <repo>:/repo:ro ghflow/app:dev python /repo/scripts/bench_history.py restore

Each prints one JSON record (all runs, medians, IQRs) for automation/runs/m3-s3/.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any

RUNS = 5
IMAGE_PROJECT = Path("/opt/ghflow/dbt/ghflow")  # the image's dbt project: the "before"
GOLD_FILES = [
    "macros/scd2_from_events.sql",
    "models/gold/dim_repo.sql",
    "models/gold/dim_actor.sql",
]
DIMS = ("dim_repo", "dim_actor")


def spread(values: list[float]) -> dict[str, Any]:
    q = statistics.quantiles(values, n=4)
    return {"all_s": [round(v, 2) for v in values], "median_s": round(statistics.median(values), 2),
            "iqr_s": [round(q[0], 2), round(q[2], 2)]}  # fmt: skip


def dbt_run(project: Path, warehouse: Path) -> float:
    dbt = "/opt/ghflow-venv/bin/dbt"
    args = [dbt, "run", "--project-dir", str(project), "--profiles-dir", str(project),
            "--select", *DIMS, "--target-path", f"/tmp/target-{project.name}",
            "--log-path", f"/tmp/logs-{project.name}"]  # fmt: skip
    env = {**os.environ, "GHFLOW_WAREHOUSE": str(warehouse), "HOME": "/tmp"}
    started = time.monotonic()
    done = subprocess.run(args, capture_output=True, text=True, env=env)
    if done.returncode != 0:
        raise RuntimeError(done.stdout[-3000:])
    return time.monotonic() - started


def unequal_rows(warehouse: Path) -> dict[str, int]:
    import duckdb

    with duckdb.connect(str(warehouse)) as con:
        return {
            dim: con.execute(
                f"select (select count(*) from (select * from main_gold.{dim} except all "
                f"select * from ref_{dim})) + (select count(*) from (select * from ref_{dim} "
                f"except all select * from main_gold.{dim}))"
            ).fetchone()[0]  # type: ignore[index]
            for dim in DIMS
        }


def keep_reference(warehouse: Path) -> None:
    import duckdb

    with duckdb.connect(str(warehouse)) as con:
        for dim in DIMS:
            con.execute(f"create or replace table ref_{dim} as select * from main_gold.{dim}")


def scd2() -> dict[str, Any]:
    work = Path("/tmp/bench")
    shutil.rmtree(work, ignore_errors=True)
    before, after = work / "before", work / "after"
    shutil.copytree(IMAGE_PROJECT, before)
    shutil.copytree(IMAGE_PROJECT, after)
    for name in GOLD_FILES:
        shutil.copy(Path("/repo/dbt/ghflow") / name, after / name)
    warehouse = work / "ghflow.duckdb"  # the views name their catalog after the file (gotcha #47)
    shutil.copy("/wh/ghflow.duckdb", warehouse)
    times: dict[str, list[float]] = {"before": [], "after": []}
    unequal = []
    for _ in range(RUNS):
        times["before"].append(dbt_run(before, warehouse))  # table: a full recompute
        keep_reference(warehouse)
        times["after"].append(dbt_run(after, warehouse))  # incremental on top of the full table
        unequal.append(unequal_rows(warehouse))
    return {"benchmark": "scd2", "metric": "wall s of `dbt run --select dim_repo dim_actor`",
            "before": spread(times["before"]), "after": spread(times["after"]),
            "after_vs_full_unequal_rows": unequal}  # fmt: skip


BEFORE_RESTORE = """
import time
from pathlib import Path
from ghflow.stream import clients, repo_state
t = time.monotonic()
store = repo_state.StateStore(Path("/tmp/before.sqlite"))  # M3-S2: always a fresh store
read = repo_state.restore(clients.changelog_reader(), store)
print(time.monotonic() - t, read)
"""
AFTER_RESTORE = """
import time
from pathlib import Path
from ghflow.stream import repo_state
t = time.monotonic()
store, opened = repo_state.open_store(Path("/tmp/after.sqlite"))
print(time.monotonic() - t, opened["restored_records"], opened["mode"].replace(" ", "_"))
"""


def timed(code: str, src: str) -> list[str]:
    """One side in its own interpreter, with its own code first on the path, on its own clock."""
    env = {**os.environ, "PYTHONPATH": src}
    done = subprocess.run(["python", "-c", code], capture_output=True, text=True, env=env)
    if done.returncode != 0:
        raise RuntimeError(done.stderr[-3000:])
    return done.stdout.split()


def restore(tail: bool = False) -> dict[str, Any]:
    """before = M3-S2's start, a full rebuild (/before: a worktree of main); after = this
    branch's start on a store it already holds, as a processor restarted while caught up finds
    it. With `tail` (M4-S3, #36) both sides start on that held store: main's code against this
    branch's. The store is prepared once, untimed."""
    setup = timed(AFTER_RESTORE, "/repo/src")
    before_code = AFTER_RESTORE if tail else BEFORE_RESTORE
    times: dict[str, list[float]] = {"before": [], "after": []}
    records: dict[str, list[Any]] = {"before": [], "after": []}
    sides = (("before", before_code, "/before/src"), ("after", AFTER_RESTORE, "/repo/src"))
    for _ in range(RUNS):
        for side, code, src in sides:
            out = timed(code, src)
            times[side].append(float(out[0]))
            records[side].append(out[1:])
    return {"benchmark": "restore", "metric": "s from process start to a usable store",
            "setup_of_the_after_store": setup, "before": spread(times["before"]),
            "after": spread(times["after"]), "records_read_and_mode": records}  # fmt: skip


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("benchmark", choices=["scd2", "restore"])
    parser.add_argument(
        "--tail", action="store_true", help="restore: both sides start on a held store"
    )
    args = parser.parse_args()
    record = scd2() if args.benchmark == "scd2" else restore(args.tail)
    print(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()

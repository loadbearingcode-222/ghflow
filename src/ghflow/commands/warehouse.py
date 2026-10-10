"""Compact the DuckDB warehouse: copy it into a fresh file and swap it in (M2-S4, #26).

    python -m ghflow.commands.warehouse compact    # in the stack, inside the `duckdb` pool

DuckDB never shrinks its file, and a full refresh writes the new staging table before dropping
the old one, so the file grew 7.45 -> 26.2 GB over five refreshes (M1-S3, M1-S5). `COPY FROM
DATABASE` into a new file writes only live data; `Path.replace` swaps it in atomically. The views
name their catalog after the file (`ghflow`, gotcha #47), so the swapped-in file keeps the name.
It must run where nothing else writes the warehouse: dbt_build's `compact` task, in the one-slot
`duckdb` pool (ADR-0003).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from ghflow.exitcodes import ExitCode, guarded

WAREHOUSE = Path(os.environ.get("GHFLOW_WAREHOUSE", "/opt/ghflow/warehouse/ghflow.duckdb"))
STAGING = "main_staging.stg_gharchive__events"


def compact(path: Path) -> dict[str, Any]:
    import duckdb  # the dbt group's dependency; present wherever the warehouse is

    fresh = path.with_name(path.stem + ".compact.duckdb")
    fresh.unlink(missing_ok=True)
    started, before = time.monotonic(), path.stat().st_size
    with duckdb.connect() as con:
        con.execute(f"attach '{path}' as ghflow (read_only)")
        rows = con.execute(f"select count(*) from ghflow.{STAGING}").fetchone()
        con.execute(f"attach '{fresh}' as fresh")
        con.execute("copy from database ghflow to fresh")
        copied = con.execute(f"select count(*) from fresh.{STAGING}").fetchone()
    if rows != copied:
        fresh.unlink(missing_ok=True)
        raise RuntimeError(f"compaction lost rows: {rows} -> {copied}; the warehouse is untouched")
    fresh.replace(path)  # atomic on one filesystem
    return {
        "bytes_before": before,
        "bytes_after": path.stat().st_size,
        "staging_rows": rows[0] if rows else None,
        "seconds": round(time.monotonic() - started, 1),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["compact"])
    parser.parse_args(argv)

    def go() -> int:
        print(json.dumps(compact(WAREHOUSE)))
        return ExitCode.PASSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

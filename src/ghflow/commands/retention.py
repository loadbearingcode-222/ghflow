"""Delete raw .gz files older than settings.RAW_KEEP_DAYS; the `raw_retention` DAG runs it daily.

    python -m ghflow.commands.retention --as-of 2026-10-05T00:00:00+00:00 --dry-run
    python -m ghflow.commands.retention --as-of 2026-10-05T00:00:00+00:00

Only the raw bucket is ever listed, so bronze cannot be touched. The dry run prints exactly the
list the real run would delete.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

from ghflow import retention
from ghflow.commands.local import s3_config
from ghflow.exitcodes import ExitCode, guarded
from ghflow.storage import Store


def run(store: Store, as_of: datetime, dry_run: bool) -> int:
    planned = retention.plan(store.files(retention.PREFIX.rstrip("/")), as_of)
    if not dry_run:
        for path in planned.expired:
            store.delete(path)
    print(
        json.dumps(
            {
                "as_of": as_of.isoformat(),
                "cutoff": planned.cutoff.isoformat(),
                "dry_run": dry_run,
                "deleted" if not dry_run else "would_delete": planned.expired,
                "kept": len(planned.kept),
                "unrecognised": planned.unrecognised,
            }
        )
    )
    return ExitCode.PASSED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--as-of", required=True, type=datetime.fromisoformat)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.as_of.tzinfo is None:
        parser.error("--as-of needs a UTC offset")
    return guarded(lambda: run(Store(s3_config()), args.as_of, args.dry_run))


if __name__ == "__main__":
    sys.exit(main())

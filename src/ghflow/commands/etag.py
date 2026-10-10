"""Re-check published ETags and queue re-published hours for re-ingest (M1-S4, ADR-0004).

    python -m ghflow.commands.etag check --days 14 [--audit-table T] [--dry-run]
    python -m ghflow.commands.etag process      # re-ingest pending requests (real audit only)
    python -m ghflow.commands.etag redteam      # a planted ETag in a COPY of the audit table

An hour is re-published when the archive now serves it with an ETag other than the one its
latest accepted ingest recorded. `check` HEADs the last N days of hours (the sweep's politeness
applies) and queues each re-published hour in `reingest_requests`; `process` re-runs ingest for
requests from the real audit table and marks them done.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

from ghflow import audit
from ghflow.archive import Verdict, sweep
from ghflow.commands import ingest
from ghflow.commands.local import ops_dsn, s3_config
from ghflow.exitcodes import ExitCode, guarded
from ghflow.hours import Hour, hours_between
from ghflow.quality.etag import republished
from ghflow.storage import Store

REDTEAM_TABLE = "ingest_audit_redteam"


def recent_hours(days: int) -> list[Hour]:
    """Every complete hour of the last `days` days (the current hour is not yet published)."""
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    return hours_between(Hour(now - timedelta(days=days)), Hour(now - timedelta(hours=1)))


def check(conn: Any, days: int, table: str, dry_run: bool) -> dict[str, Any]:
    hours = recent_hours(days)
    recorded = audit.recorded_etags(conn, hours[0].start, hours[-1].plus(1).start, table)
    probes = sweep([h for h in hours if h.start in recorded])
    live = {p.hour: p.etag if p.verdict is Verdict.PUBLISHED else None for p in probes}
    by_hour = {Hour(start): etag for start, etag in recorded.items()}
    changed = republished(by_hour, live)
    if not dry_run:
        for hour in changed:
            audit.request_reingest(conn, hour, by_hour[hour], live[hour], table)
    return {
        "audit_table": table,
        "hours_checked": len(live),
        "probes_failed": sum(v is None for v in live.values()),
        "republished": [h.key for h in changed],
        "scheduled": not dry_run,
    }


def process(conn: Any) -> dict[str, Any]:
    store, done = Store(s3_config()), []
    for request_id, key in audit.pending_reingests(conn):
        code = ingest.run(Hour.parse(key), None, store, conn)
        if code == ExitCode.PASSED:
            audit.mark_reingested(conn, request_id)
            done.append(key)
    return {"reingested": done}


def redteam(conn: Any, days: int) -> dict[str, Any]:
    """Copy the audit table, change one recent hour's recorded ETag in the copy, check the copy."""
    conn.execute(f"drop table if exists {REDTEAM_TABLE}".encode())
    conn.execute(f"create table {REDTEAM_TABLE} as select * from ingest_audit".encode())
    try:
        hours = recent_hours(days)
        target = hours[len(hours) // 2]
        conn.execute(
            f"update {REDTEAM_TABLE} set etag = '\"planted\"' where hour = %s and outcome = 'accepted'".encode(),
            (target.key,),
        )
        result = check(conn, days, REDTEAM_TABLE, dry_run=False)
        scheduled = conn.execute(
            b"select hour from reingest_requests where audit_table = %s and requested_at > now() - interval '1 hour'",
            (REDTEAM_TABLE,),
        ).fetchall()
    finally:
        conn.execute(f"drop table if exists {REDTEAM_TABLE}".encode())
    keys = sorted({r[0] for r in scheduled})
    return {
        "planted_hour": target.key,
        **result,
        "scheduled_from_copy": keys,
        "caught": keys == [target.key],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["check", "process", "redteam"])
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--audit-table", default="ingest_audit")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    def go() -> int:
        with audit.connect(ops_dsn()) as conn:
            if args.command == "check":
                record = check(conn, args.days, args.audit_table, args.dry_run)
            elif args.command == "process":
                record = process(conn)
            else:
                record = redteam(conn, args.days)
        print(json.dumps(record))
        return ExitCode.REFUSED if record.get("caught") is False else ExitCode.PASSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

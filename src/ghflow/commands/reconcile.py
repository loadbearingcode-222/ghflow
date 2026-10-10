"""Reconcile batch and stream for every hour of a window (M2-S3, ADR-0007).

    python -m ghflow.commands.reconcile run --to 2026-10-10-1 [--from 2026-08-03-0]
    python -m ghflow.commands.reconcile redteam --to 2026-08-03-5   # one event dropped, in a copy

Writes one row per hour to ghflow_ops.reconcile_results (status, counts, named ids) and exits 1
if any hour is red. Reads only the `id` column of each Parquet file, eight hours at a time.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from typing import Any

import pyarrow.parquet as pq

from ghflow import audit, bronze, settings
from ghflow.commands.local import ops_dsn, s3_config
from ghflow.exitcodes import ExitCode, guarded
from ghflow.hours import Hour, hours_between
from ghflow.reconcile import HourCheck, compare
from ghflow.storage import Store
from ghflow.stream import lander

RESULTS_SQL = b"""
create table if not exists reconcile_results (
    id             bigserial primary key,
    run_id         text        not null,
    checked_at     timestamptz not null default now(),
    hour           text        not null,
    status         text        not null check (status in ('green', 'red', 'pending')),
    batch_ids      bigint      not null,
    stream_ids     bigint      not null,
    missing_count  bigint      not null,
    extra_count    bigint      not null,
    duplicates     bigint,
    missing        bigint[]    not null,
    extra          bigint[]    not null
);
"""


def ids(store: Store, path: str) -> list[int] | None:
    if not store.exists(path):
        return None
    return list(pq.read_table(io.BytesIO(store.get(path)), columns=["id"]).column("id").to_pylist())


def check_hour(store: Store, hour: Hour, duplicates: dict[str, int], stream_path: str) -> HourCheck:
    batch = ids(store, bronze.bronze_path(hour)) or []
    return compare(hour.key, batch, ids(store, stream_path), duplicates.get(hour.key))


def reconcile(conn: Any, hours: list[Hour], overrides: dict[str, str]) -> dict[str, Any]:
    store, run_id = Store(s3_config()), f"reconcile-{uuid.uuid4().hex[:8]}"
    duplicates = {
        r[0]: int(r[1])
        for r in conn.execute(b"select hour, duplicates from stream_compacted").fetchall()
    }
    with ThreadPoolExecutor(8) as pool:
        checks = list(
            pool.map(
                lambda h: check_hour(
                    store, h, duplicates, overrides.get(h.key, lander.compact_path(h.key))
                ),
                hours,
            )
        )
    with conn.cursor() as cur:
        cur.executemany(
            b"insert into reconcile_results (run_id, hour, status, batch_ids, stream_ids, missing_count, "
            b"extra_count, duplicates, missing, extra) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    run_id,
                    c.hour,
                    c.status,
                    c.batch_ids,
                    c.stream_ids,
                    c.missing_count,
                    c.extra_count,
                    c.duplicates,
                    c.missing,
                    c.extra,
                )
                for c in checks
            ],
        )
    red = [asdict(c) for c in checks if c.status == "red"]
    return {
        "run_id": run_id,
        "window": [hours[0].key, hours[-1].key],
        "hours": {s: sum(c.status == s for c in checks) for s in ("green", "red", "pending")},
        "events": {
            "batch": sum(c.batch_ids for c in checks),
            "stream": sum(c.stream_ids for c in checks),
        },
        "missing": sum(c.missing_count for c in checks),
        "extra": sum(c.extra_count for c in checks),
        "duplicates_removed_by_compaction": {c.hour: c.duplicates for c in checks if c.duplicates},
        "red": red,
    }


def redteam(conn: Any, hours: list[Hour]) -> dict[str, Any]:
    """Drop one event from a disposable copy of one compacted hour; reconcile with the copy."""
    store = Store(s3_config())
    target = hours[len(hours) // 2]
    table = pq.read_table(io.BytesIO(store.get(lander.compact_path(target.key))))
    dropped = table.column("id")[0].as_py()
    scratch = f"{lander.BUCKET}/redteam/{uuid.uuid4().hex}/events.parquet"
    store.put(scratch, lander_encode(table.slice(1), target.key))
    try:
        result = reconcile(conn, hours, {target.key: scratch})
    finally:
        store.delete(scratch)
    named = [(r["hour"], r["missing"]) for r in result["red"]]
    caught = named == [(target.key, [dropped])] and result["hours"]["green"] == len(hours) - 1
    return {
        "planted": {"hour": target.key, "dropped_event_id": dropped},
        "result": result,
        "caught": caught,
    }


def lander_encode(table: Any, hour_key: str) -> bytes:
    from ghflow.stream import compact

    return compact.encode(table, hour_key)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["run", "redteam"])
    parser.add_argument("--from", dest="first", default=f"{settings.WINDOW_FIRST.isoformat()}-0")
    parser.add_argument("--to", required=True)
    args = parser.parse_args(argv)
    hours = hours_between(Hour.parse(args.first), Hour.parse(args.to))

    def go() -> int:
        with audit.connect(ops_dsn()) as conn:
            conn.execute(RESULTS_SQL)
            record = reconcile(conn, hours, {}) if args.command == "run" else redteam(conn, hours)
        print(json.dumps(record, default=str))
        if args.command == "redteam":
            return ExitCode.PASSED if record["caught"] else ExitCode.REFUSED
        return ExitCode.REFUSED if record["hours"]["red"] else ExitCode.PASSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

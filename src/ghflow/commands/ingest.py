"""Ingest one GH Archive hour into bronze; the `gharchive_ingest` DAG runs this per data interval.

    python -m ghflow.commands.ingest probe 2026-10-05-14   # 0 published · 3 not yet · 2 request failed
    python -m ghflow.commands.ingest run 2026-10-05-14     # 0 accepted · 1 refused · 2 could not fetch
    python -m ghflow.commands.ingest run 2020-01-01-0 --source planted.json.gz   # red team, no network

One run: GET the hour → land the raw .gz in S3 → apply the contract → write the bronze Parquet
and the rejected-row sidecar → append one audit row. A refused hour writes no bronze and exits 1.
Every attempt leaves exactly one audit row, whatever happens (a crash is recorded, then raised).
Re-running an hour overwrites the same objects with identical bytes and appends a new row.
"""

from __future__ import annotations

import argparse
import gzip
import http.client
import io
import json
import os
import sys
import time
import urllib.error
from pathlib import Path
from typing import Any

from ghflow import audit, bronze
from ghflow.archive import Download, Verdict, fetch_hour, head
from ghflow.contract import HourResult, Refusal, apply
from ghflow.exitcodes import ExitCode, guarded
from ghflow.hours import Hour
from ghflow.storage import S3Config, Store

#: A download that did not complete: retried by Airflow, recorded as `failed`, never `crashed`.
#: http.client.IncompleteRead (a dropped connection mid-body) is not an OSError (M1-S2 backfill).
TRANSPORT_ERRORS = (urllib.error.URLError, OSError, http.client.HTTPException)

PROBE_EXIT = {
    Verdict.PUBLISHED: ExitCode.PASSED,
    Verdict.NOT_PUBLISHED: ExitCode.NO_VERDICT,
    Verdict.REQUEST_FAILED: ExitCode.COULD_NOT_BUILD,
}


def probe(hour: Hour) -> int:
    result = head(hour)
    print(json.dumps({"hour": hour.key, "verdict": result.verdict.value, "size": result.size}))
    return PROBE_EXIT[result.verdict]


def fetch(hour: Hour, source: Path | None) -> Download:
    if source is not None:
        return Download(hour, source.read_bytes(), etag=None, last_modified=None)
    return fetch_hour(hour)


def contract_of(download: Download) -> HourResult:
    """The contract over the hour's lines, streamed out of the in-memory .gz."""
    with gzip.GzipFile(fileobj=io.BytesIO(download.data)) as lines:
        result = apply(lines, download.hour)
    if result.lines_in != len(result.rows) + len(result.rejected):
        raise AssertionError(f"{download.hour.key}: rows in != accepted + rejected")
    return result


def land(download: Download, attempt: audit.Attempt, store: Store, landed_raw: bool) -> int:
    """Everything after the fetch; fills `attempt` and returns the exit code."""
    hour = download.hour
    if landed_raw:
        attempt.raw_sha256 = store.put(bronze.raw_path(hour), download.data)
    try:
        result = contract_of(download)
    except Refusal as refusal:
        attempt.outcome, attempt.refusal_code, attempt.detail = (
            "refused",
            refusal.code,
            refusal.detail,
        )
        return ExitCode.REFUSED
    attempt.rows_in, attempt.rows_accepted = result.lines_in, len(result.rows)
    attempt.rows_rejected = len(result.rejected)
    attempt.bronze_sha256 = store.put(
        bronze.bronze_path(hour), bronze.events_parquet(result.rows, hour)
    )
    attempt.sidecar_sha256 = store.put(
        bronze.sidecar_path(hour), bronze.sidecar_parquet(result.rejected, hour)
    )
    attempt.outcome = "accepted"
    return ExitCode.PASSED


def run(hour: Hour, source: Path | None, store: Store, conn: Any) -> int:
    started = time.monotonic()
    attempt = audit.Attempt(hour.key, hour.start, f"file:{source.name}" if source else "archive")
    try:
        try:
            download = fetch(hour, source)
        except TRANSPORT_ERRORS as exc:
            attempt.outcome, attempt.detail = "failed", f"{type(exc).__name__}: {exc}"[:500]
            return ExitCode.COULD_NOT_BUILD
        attempt.http_verdict = Verdict.PUBLISHED.value if source is None else None
        attempt.etag, attempt.last_modified = download.etag, download.last_modified
        attempt.content_length = len(download.data)
        return land(download, attempt, store, landed_raw=source is None)
    except BaseException as exc:  # recorded as crashed WITH its reason, then raised
        attempt.detail = f"{type(exc).__name__}: {exc}"[:500]
        raise
    finally:
        attempt.duration_ms = int((time.monotonic() - started) * 1000)
        audit.record(conn, attempt)
        print(json.dumps({k: v for k, v in vars(attempt).items() if k != "hour_start"}))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["probe", "run"])
    parser.add_argument("hour", help="GH Archive hour key, e.g. 2026-10-05-14")
    parser.add_argument("--source", type=Path, help="read this local .json.gz instead (red team)")
    args = parser.parse_args(argv)
    hour = Hour.parse(args.hour)

    def go() -> int:
        if args.command == "probe":
            return probe(hour)
        with audit.connect(os.environ["GHFLOW_OPS_DSN"]) as conn:
            return run(hour, args.source, Store(S3Config.from_env()), conn)

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

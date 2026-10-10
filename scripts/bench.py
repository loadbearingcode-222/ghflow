"""Named benchmarks on cached reference hours, one record per story (docs/ITERATION.md).

    uv run python scripts/bench.py contract --out automation/runs/m0-s5/bench-before.json

Every record carries what makes a before/after comparison honest:
- the input's sha256 (the cached .json.gz), so "before" and "after" ran on the same bytes;
- at least five timed runs, reported as median with min/max spread, so noise is visible;
- an output fingerprint (sha256 of the result), so a change that buys speed by changing
  behaviour shows up as a different fingerprint, not as a win.

Reference hours are downloaded once into the user cache directory, never into the repo.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

from ghflow.archive import download_hour
from ghflow.contract import apply
from ghflow.events import parse
from ghflow.hours import Hour

#: A typical recent hour and the heaviest broken-mix hour (both inside the backfill window).
REFERENCE_HOURS = ("2026-10-02-15", "2026-09-05-15")
MIN_RUNS = 5


def cache_dir() -> Path:
    base = os.environ.get("LOCALAPPDATA") or str(Path.home() / ".cache")
    return Path(base) / "ghflow" / "reference-hours"


def reference(key: str) -> tuple[list[bytes], str]:
    """The hour's decompressed lines and the sha256 of its cached .json.gz."""
    path = cache_dir() / f"{key}.json.gz"
    if not path.exists():
        download_hour(Hour.parse(key), path)
    raw = path.read_bytes()
    return gzip.decompress(raw).splitlines(keepends=True), hashlib.sha256(raw).hexdigest()


def fingerprint(rows: list[dict[str, Any]], report: dict[str, Any]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, sort_keys=True, default=str).encode())
        digest.update(b"\n")
    digest.update(json.dumps(report, sort_keys=True).encode())
    return digest.hexdigest()


def timed(fn: Callable[[], object], runs: int) -> list[float]:
    seconds = []
    for _ in range(runs):
        start = time.perf_counter()
        fn()
        seconds.append(time.perf_counter() - start)
    return seconds


def summary(seconds: list[float], rows: int) -> dict[str, Any]:
    median = statistics.median(seconds)
    return {
        "runs": len(seconds),
        "median_s": round(median, 4),
        "min_s": round(min(seconds), 4),
        "max_s": round(max(seconds), 4),
        "spread_pct": round(100 * (max(seconds) - min(seconds)) / median, 1),
        "rows_per_s": round(rows / median),
        "all_s": [round(s, 4) for s in seconds],
    }


def parse_all(lines: list[bytes]) -> list[Any]:
    """The standard library's parse alone: the reference cost, comparable across versions."""
    return [json.loads(line) for line in lines]


def read_all(lines: list[bytes]) -> list[Any]:
    """The one reader's parse alone (ghflow.events, from M0-S5)."""
    return [parse(line) for line in lines]


def bench_contract(runs: int, hours: list[str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for key in hours:
        lines, input_sha = reference(key)
        hour = Hour.parse(key)
        result = apply(lines, hour)
        parse_only = timed(partial(parse_all, lines), runs)
        reader_only = timed(partial(read_all, lines), runs)
        full = timed(partial(apply, lines, hour), runs)
        results.append(
            {
                "hour": key,
                "input_sha256": input_sha,
                "rows_in": len(lines),
                "raw_mb": round(sum(map(len, lines)) / 1e6, 1),
                "contract_apply": summary(full, len(lines)),
                "json_parse_only": summary(parse_only, len(lines)),
                "reader_parse_only": summary(reader_only, len(lines)),
                "parse_share_pct": round(
                    100 * statistics.median(parse_only) / statistics.median(full), 1
                ),
                "output_sha256": fingerprint(result.rows, result.report()),
            }
        )
        print(f"{key}: apply median {results[-1]['contract_apply']['median_s']} s", file=sys.stderr)
    return results


#: The fixed day for the ingest benchmark (M1-S5): 24 cached hours, the contract reference day.
INGEST_DAY = "2026-10-02"


def ingest_once(hours: list[tuple[Hour, bytes]]) -> list[str]:
    """One hour after another, exactly as ingest lands it minus the S3 PUT: the bronze sha256s."""
    from ghflow import bronze
    from ghflow.archive import Download
    from ghflow.commands.ingest import contract_of

    shas = []
    for hour, data in hours:
        result = contract_of(Download(hour, data, None, None))
        events = bronze.events_parquet(result.rows, hour)
        bronze.sidecar_parquet(result.rejected, hour)
        shas.append(hashlib.sha256(events).hexdigest())
    return shas


def bench_ingest_day(runs: int, _hours: list[str]) -> list[dict[str, Any]]:
    """Ingest compute throughput for a fixed day: decode + contract + Parquet, 24 cached hours.

    The download and the S3 PUT are left out on purpose: they measure the network and the disk,
    and would drown a code change in noise. The record says so.
    """
    hours = []
    for h in range(24):
        hour = Hour.parse(f"{INGEST_DAY}-{h}")
        path = cache_dir() / f"{hour.key}.json.gz"
        if not path.exists():
            download_hour(hour, path)
        hours.append((hour, path.read_bytes()))
    shas = ingest_once(hours)
    seconds = timed(partial(ingest_once, hours), runs)
    rows = sum(len(gzip.decompress(d).splitlines()) for _, d in hours)
    inputs = hashlib.sha256("".join(hashlib.sha256(d).hexdigest() for _, d in hours).encode())
    return [
        {
            "day": INGEST_DAY,
            "hours": len(hours),
            "rows_in": rows,
            "gz_mb": round(sum(len(d) for _, d in hours) / 1e6, 1),
            "inputs_sha256": inputs.hexdigest(),
            "excluded": "download and S3 PUT (network and disk, not code)",
            "ingest_day": summary(seconds, rows),
            "bronze_sha256_by_hour": dict(zip([h.key for h, _ in hours], shas, strict=True)),
        }
    ]


def bench_stream(runs: int, hours: list[str]) -> list[dict[str, Any]]:
    """Replayer and lander events/s (scripts/bench_stream.py, kept apart for the size budget)."""
    from bench_stream import bench_stream as run

    return run(runs, hours)


BENCHMARKS: dict[str, Callable[[int, list[str]], list[dict[str, Any]]]] = {
    "contract": bench_contract,
    "ingest-day": bench_ingest_day,
    "stream": bench_stream,
}


def git_head() -> str:
    out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True)
    return out.stdout.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("benchmark", choices=sorted(BENCHMARKS))
    parser.add_argument("--runs", type=int, default=MIN_RUNS)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--hours", nargs="+", default=list(REFERENCE_HOURS), help="reference hour keys"
    )
    args = parser.parse_args(argv)
    if args.runs < MIN_RUNS:
        parser.error(f"--runs must be at least {MIN_RUNS}: fewer cannot show the spread")
    record = {
        "benchmark": args.benchmark,
        "recorded_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git_head": git_head(),
        "machine": {"python": platform.python_version(), "processor": platform.processor()},
        "results": BENCHMARKS[args.benchmark](args.runs, args.hours),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

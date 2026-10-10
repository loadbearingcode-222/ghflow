"""Event-mix and volume quality, judged against a rolling clean baseline (M1-S4, ADR-0004).

    python -m ghflow.commands.quality counts --out automation/runs/m1-s4/hour_counts.json   # in the stack
    python -m ghflow.commands.quality argue  --counts ... --out automation/runs/m1-s4/seed_spread.json
    python -m ghflow.commands.quality run    --counts ... --out automation/runs/m1-s4/verdicts.json [--only volume]
    python -m ghflow.commands.quality redteam --counts ... --out automation/runs/m1-s4/redteam_mix.json
    python -m ghflow.commands.quality judge  --run-id <id>   # in the stack: warehouse -> quality_verdicts

`counts` reads the warehouse read-only; `argue` and `run` need only the committed counts file,
so anyone can re-derive the verdicts without the stack.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from statistics import quantiles
from typing import Any

from ghflow import settings
from ghflow.exitcodes import ExitCode, guarded
from ghflow.hours import Hour, hours_of_days
from ghflow.quality.baseline import HourCounts, median_rows, median_share, reference
from ghflow.quality.checks import CHECKS, judge_all

#: Red team: a clean, normal hour is given the reference broken hour's event mix.
REDTEAM_TARGET, REDTEAM_DONOR = "2026-08-23-12", "2026-09-05-15"

WAREHOUSE = "/opt/ghflow/warehouse/ghflow.duckdb"


def seed() -> set[Hour]:
    return set(hours_of_days(settings.QUALITY_SEED_FIRST, settings.QUALITY_SEED_LAST))


def extract_counts(warehouse: str) -> dict[str, dict[str, int]]:
    import duckdb  # the dbt group's dependency; present wherever the warehouse is

    with duckdb.connect(warehouse, read_only=True) as con:
        rows = con.execute(
            "select strftime(event_date, '%Y-%m-%d') || '-' || event_hour, type, count(*) "
            "from ghflow.main_staging.stg_gharchive__events group by all"
        ).fetchall()
    out: dict[str, dict[str, int]] = {}
    for key, event_type, n in rows:
        out.setdefault(key, {})[event_type] = int(n)
    return dict(sorted(out.items(), key=lambda kv: Hour.parse(kv[0])))


def load(path: Path) -> list[HourCounts]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [HourCounts(Hour.parse(k), v) for k, v in data["hours"].items()]


def spread(values: list[float]) -> dict[str, Any]:
    cuts = quantiles(values, n=200, method="inclusive")  # 0.5% steps
    return {"n": len(values), "min": round(min(values), 4), "p0.5": round(cuts[0], 4),
            "p2.5": round(cuts[4], 4), "median": round(cuts[99], 4), "p97.5": round(cuts[194], 4),
            "p99.5": round(cuts[198], 4), "max": round(max(values), 4)}  # fmt: skip


def argue(counts: list[HourCounts]) -> dict[str, Any]:
    """How far clean-looking seed hours sit from their own reference: the evidence for the bars."""
    history = {c.hour: c for c in counts}
    hours = sorted(h for h in seed() if h in history)
    rows: list[float] = []
    shares: dict[str, list[float]] = {}
    worst: list[tuple[str, str, float]] = []
    for hour in hours:
        ref = reference(hour, history, clean=set(), seed=set(hours))
        ratio = history[hour].rows / median_rows(ref)
        rows.append(ratio)
        worst.append((hour.key, "rows", round(ratio, 4)))
        for event_type in {t for r in ref for t in r.types}:
            expected = median_share(ref, event_type)
            if expected >= settings.QUALITY_MIX_MIN_SHARE:
                r = history[hour].share(event_type) / expected
                shares.setdefault(event_type, []).append(r)
                worst.append((hour.key, event_type, round(r, 4)))
    extremes = sorted(worst, key=lambda w: abs(w[2] - 1), reverse=True)[:20]
    return {
        "seed": [hours[0].key, hours[-1].key],
        "mix_min_share": settings.QUALITY_MIX_MIN_SHARE,
        "volume_ratio": spread(rows),
        "share_ratio_by_type": {t: spread(v) for t, v in sorted(shares.items())},
        "furthest_from_reference": extremes,
    }


def run(counts: list[HourCounts], only: str | None) -> dict[str, Any]:
    checks = (only,) if only else CHECKS
    verdicts = judge_all(counts, seed(), checks)
    flagged = [v for v in verdicts if v.status == "flagged"]
    return {
        "checks": list(checks),
        "bars": {
            k: getattr(settings, k)
            for k in dir(settings)
            if k.startswith("QUALITY_") and not k.startswith("QUALITY_SEED")
        },
        "summary": {
            s: sum(v.status == s for v in verdicts)
            for s in ("clean", "flagged", "shifted", "no_verdict")
        },
        "flagged": {v.hour.key: [asdict(f) for f in v.findings] for v in flagged},
        "shifted": {
            v.hour.key: [asdict(f) for f in v.findings] for v in verdicts if v.status == "shifted"
        },
        "no_verdict": [v.hour.key for v in verdicts if v.status == "no_verdict"],
    }


def redteam(counts: list[HourCounts]) -> dict[str, Any]:
    """Plant a broken hour's mix (scaled to the target's own row count) into a clean hour."""
    by = {c.hour: c for c in counts}
    target, donor = Hour.parse(REDTEAM_TARGET), Hour.parse(REDTEAM_DONOR)
    rows = by[target].rows
    planted = {t: round(n / by[donor].rows * rows) for t, n in by[donor].types.items()}
    before = {v.hour: v for v in judge_all(counts, seed())}
    after = {
        v.hour: v
        for v in judge_all(
            [HourCounts(c.hour, planted) if c.hour == target else c for c in counts], seed()
        )
    }
    changed = [h.key for h in before if h != target and before[h].status != after[h].status]
    caught = after[target].status == "flagged" and not changed
    return {
        "target": REDTEAM_TARGET, "donor_mix": REDTEAM_DONOR, "rows": [rows, sum(planted.values())],
        "before": before[target].status, "after": after[target].status,
        "findings": [asdict(f) for f in after[target].findings],
        "other_hours_changed": changed, "caught": caught,
    }  # fmt: skip


def judge(run_id: str) -> dict[str, Any]:
    """Judge every hour in the warehouse and record each verdict, with its bars, in ghflow_ops."""
    from ghflow import audit
    from ghflow.commands.local import ops_dsn

    hours = extract_counts(os.environ.get("GHFLOW_WAREHOUSE", WAREHOUSE))
    counts = [HourCounts(Hour.parse(k), v) for k, v in hours.items()]
    result = run(counts, None)
    verdicts = judge_all(counts, seed())
    rows = [
        (v.hour.key, v.hour.start, v.status, json.dumps([asdict(f) for f in v.findings]))
        for v in verdicts
    ]
    with audit.connect(ops_dsn()) as conn:
        audit.record_verdicts(conn, run_id, rows, json.dumps(result["bars"]))
    return {"run_id": run_id, "summary": result["summary"]}


def write(path: Path | None, record: dict[str, Any]) -> None:
    text = json.dumps(record, indent=2, default=str) + "\n"
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="\n")
    else:
        print(text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["counts", "argue", "run", "redteam", "judge"])
    parser.add_argument("--counts", type=Path, help="argue/run: the hour_counts.json to judge")
    parser.add_argument(
        "--only", choices=CHECKS, help="run: one check alone (e.g. the volume control)"
    )
    parser.add_argument("--out", type=Path)
    parser.add_argument("--run-id", help="judge: the Airflow run id the verdicts are filed under")
    args = parser.parse_args(argv)

    def go() -> int:
        if args.command == "counts":
            hours = extract_counts(os.environ.get("GHFLOW_WAREHOUSE", WAREHOUSE))
            write(args.out, {"source": "main_staging.stg_gharchive__events", "hours": hours})
            return ExitCode.PASSED
        if args.command == "judge":
            write(None, judge(args.run_id or "manual"))
            return ExitCode.PASSED
        counts = load(args.counts)
        if args.command == "redteam":
            record = redteam(counts)
            write(args.out, record)
            return ExitCode.PASSED if record["caught"] else ExitCode.REFUSED
        write(args.out, argue(counts) if args.command == "argue" else run(counts, args.only))
        return ExitCode.PASSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

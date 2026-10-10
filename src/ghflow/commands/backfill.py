"""Backfill gharchive_ingest over the window, and park / resume / watch / verify it.

    python -m ghflow.commands.backfill start --to 2026-10-05-11   # from 2026-08-03-0 (settings)
    python -m ghflow.commands.backfill park [--wait]   # no new hours; in-flight ones finish
    python -m ghflow.commands.backfill resume
    python -m ghflow.commands.backfill watch [--every 60] [--count 0]
    python -m ghflow.commands.backfill verify --to 2026-10-05-11 [--out automation/runs/m1-s2/verdicts.json]

The backfill itself is Airflow's (scheduler-managed, `reprocess_behavior=none`, at most
settings.CONCURRENCY runs at once), so parking it is pausing it: Airflow starts no new run,
and the runs already going finish. Progress and verdicts come from the audit table, the record
of what actually happened, not from Airflow's view of it.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ghflow import audit, settings
from ghflow.commands.airflow_api import Airflow
from ghflow.commands.local import ops_dsn
from ghflow.exitcodes import ExitCode, guarded
from ghflow.hours import Hour, hours_between

DAG = "gharchive_ingest"
FINAL = {"accepted", "refused"}


def window(last: str) -> list[Hour]:
    return hours_between(Hour.of(settings.WINDOW_FIRST, 0), Hour.parse(last))


def verdict_report(
    expected: list[Hour],
    verdicts: dict[datetime, str],
    attempts: dict[datetime, int],
    finals: dict[datetime, tuple[int, int]],
) -> dict[str, Any]:
    """Every expected hour must have exactly one final verdict; anything else is named.

    - *conflicting*: final (accepted/refused) attempts that disagree (outcome or bronze sha256).
      The only failure among these: the hour does not have ONE answer.
    - *rerun*: several final attempts with one identical result: an idempotent re-run.
    - *retried*: non-final attempts (failed, crashed) before the one that counted.
    """
    n_final = {h: n for h, (n, _) in finals.items()}
    final = {h.key: verdicts[h.start] for h in expected if verdicts.get(h.start) in FINAL}
    return {
        "expected_hours": len(expected),
        "hours_with_final_verdict": len(final),
        "outcomes": dict(Counter(final.values())),
        "missing": [h.key for h in expected if h.start not in verdicts],
        "not_final": {
            h.key: verdicts[h.start]
            for h in expected
            if verdicts.get(h.start) not in FINAL | {None}
        },
        "conflicting": {
            h.key: finals[h.start][1] for h in expected if finals.get(h.start, (0, 0))[1] > 1
        },
        "rerun": {
            h.key: n
            for h in expected
            if (n := n_final.get(h.start, 0)) > 1 and finals[h.start][1] == 1
        },
        "retried": {
            h.key: n - n_final.get(h.start, 0)
            for h in expected
            if (n := attempts.get(h.start, 0)) > n_final.get(h.start, 0)
        },
        "complete": len(final) == len(expected)
        and not any(finals.get(h.start, (0, 0))[1] > 1 for h in expected),
    }


# ------------------------------------------------------------------ Airflow's side


def latest_backfill(api: Airflow, *, open_only: bool) -> dict[str, Any]:
    """The newest backfill of the DAG; with open_only, the newest one not yet completed."""
    backfills = api.call("GET", "/api/v2/backfills", dag_id=DAG)["backfills"]
    candidates = [b for b in backfills if not open_only or b.get("completed_at") is None]
    if not candidates:
        raise LookupError(f"no {'open ' if open_only else ''}backfill for {DAG}")
    return dict(max(candidates, key=lambda b: b["id"]))


def runs_in_state(api: Airflow, *states: str) -> int:
    page = api.call(
        "GET", f"/api/v2/dags/{DAG}/dagRuns", run_type="backfill", state=list(states), limit=1
    )
    return int(page["total_entries"])


def start(api: Airflow, last: str) -> int:
    hours = window(last)
    body = {
        "dag_id": DAG,
        "from_date": hours[0].start.isoformat(),
        "to_date": hours[-1].start.isoformat(),
        "max_active_runs": settings.CONCURRENCY,
        "reprocess_behavior": "none",
    }
    made = api.call("POST", "/api/v2/backfills", body)
    print(
        json.dumps(
            {
                "backfill_id": made["id"],
                "from": hours[0].key,
                "to": hours[-1].key,
                "hours": len(hours),
            }
        )
    )
    return ExitCode.PASSED


def park(api: Airflow, wait: bool) -> int:
    backfill = latest_backfill(api, open_only=True)
    api.call("PUT", f"/api/v2/backfills/{backfill['id']}/pause")
    deadline = time.monotonic() + 15 * 60
    in_flight = runs_in_state(api, "running")
    while wait and in_flight and time.monotonic() < deadline:
        print(f"parked; waiting for {in_flight} hour(s) in flight to finish", file=sys.stderr)
        time.sleep(15)
        in_flight = runs_in_state(api, "running")
    safe = in_flight == 0
    verdict = "SAFE TO WALK AWAY" if safe else "NOT YET SAFE"
    print(
        json.dumps(
            {
                "backfill_id": backfill["id"],
                "paused": True,
                "in_flight": in_flight,
                "verdict": verdict,
            }
        )
    )
    return ExitCode.PASSED if safe else ExitCode.NO_VERDICT


def resume(api: Airflow) -> int:
    backfill = latest_backfill(api, open_only=True)
    api.call("PUT", f"/api/v2/backfills/{backfill['id']}/unpause")
    print(json.dumps({"backfill_id": backfill["id"], "paused": False}))
    return ExitCode.PASSED


# ------------------------------------------------------------------ progress, from the audit


def progress(conn: Any, api: Airflow, last: str) -> dict[str, Any]:
    hours = window(last)
    end = hours[-1].plus(1).start
    verdicts = audit.final_verdicts(conn, hours[0].start, end)
    done = sum(v in FINAL for v in verdicts.values())
    now = datetime.now(UTC)
    recent = audit.attempt_counts(conn, hours[0].start, end, since=now - timedelta(minutes=10))
    backfill = latest_backfill(api, open_only=False)
    # The rate is over the last 10 minutes, or since the backfill began if that is sooner, so
    # a fresh backfill does not read as ten times slower than it is.
    began = datetime.fromisoformat(backfill["created_at"].replace("Z", "+00:00"))
    span_min = max(1.0, min(10.0, (now - began).total_seconds() / 60))
    per_min = round(sum(recent.values()) / span_min, 1)
    return {
        "at": now.strftime("%H:%M:%S"),
        "done": done,
        "of": len(hours),
        "pct": round(100 * done / len(hours), 1),
        "refused": sum(v == "refused" for v in verdicts.values()),
        "in_flight": runs_in_state(api, "running"),
        "paused": backfill.get("is_paused"),
        "completed_at": backfill.get("completed_at"),
        "hours_per_min": per_min,
        "eta_min": round((len(hours) - done) / per_min) if per_min else None,
    }


def watch(conn: Any, api: Airflow, last: str, every: int, count: int) -> int:
    shown = 0
    while True:
        print(json.dumps(progress(conn, api, last)), flush=True)
        shown += 1
        if count and shown >= count:
            return ExitCode.PASSED
        time.sleep(every)


def verify(conn: Any, last: str, out: Path | None) -> int:
    hours = window(last)
    end = hours[-1].plus(1).start
    report = verdict_report(
        hours,
        audit.final_verdicts(conn, hours[0].start, end),
        audit.attempt_counts(conn, hours[0].start, end, since=datetime(1970, 1, 1, tzinfo=UTC)),
        audit.final_attempt_counts(conn, hours[0].start, end),
    )
    report["window"] = {"first": hours[0].key, "last": hours[-1].key}
    report["totals"] = audit.totals(conn, hours[0].start, end)
    text = json.dumps(report, indent=2, default=str)
    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8", newline="\n")
    print(text)
    return ExitCode.PASSED if report["complete"] else ExitCode.REFUSED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["start", "park", "resume", "watch", "verify"])
    parser.add_argument("--to", help="last hour of the window, inclusive (e.g. 2026-10-05-11)")
    parser.add_argument("--wait", action="store_true", help="park: wait for in-flight hours")
    parser.add_argument("--every", type=int, default=60, help="watch: seconds between lines")
    parser.add_argument("--count", type=int, default=0, help="watch: stop after N lines (0: never)")
    parser.add_argument("--out", type=Path, help="verify: write the report here too")
    args = parser.parse_args(argv)
    if args.command in {"start", "watch", "verify"} and not args.to:
        parser.error(f"{args.command} needs --to")

    def go() -> int:
        if args.command == "verify":
            with audit.connect(ops_dsn()) as conn:
                return verify(conn, args.to, args.out)
        api = Airflow()
        if args.command == "start":
            return start(api, args.to)
        if args.command == "park":
            return park(api, args.wait)
        if args.command == "resume":
            return resume(api)
        with audit.connect(ops_dsn()) as conn:
            return watch(conn, api, args.to, args.every, args.count)

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

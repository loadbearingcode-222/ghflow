"""The one-command demo: a bounded window, end to end, from a clean clone (M5-S4).

    python -m ghflow.commands.demo run      # in the stack (`.\\make.ps1 demo` does it all)

Every DAG stays paused (new DAGs are, ADR-0002), so nothing outside the window is scheduled;
this drives the same commands the DAGs run, in the order the daily chain runs them:

1. ingest every hour of DEMO_FIRST..DEMO_LAST (4 at a time), then `dbt build --full-refresh`;
2. judge every hour (the seed fortnight judges itself, so five days suffice);
3. wait, with a deadline, until the stream closed every hour: the replayer (REPLAY_UNTIL bounds
   it), the lander (compacted here), ClickHouse (markers on every partition);
4. reconcile batch = stream = ClickHouse; then the marts, their twin, the export, the dashboard.

Each step's outcome is printed as one JSON line; `scripts/demo_acceptance.py` judges the result.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from ghflow import settings
from ghflow.exitcodes import ExitCode, guarded
from ghflow.hours import Hour, hours_of_days

PY = sys.executable
DBT = str(Path(PY).with_name("dbt"))
PROJECT = "/opt/ghflow/dbt/ghflow"


def window() -> list[Hour]:
    return hours_of_days(settings.DEMO_FIRST, settings.DEMO_LAST)


def sh(args: list[str], what: str) -> str:
    done = subprocess.run(args, capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(
            f"{what} exited {done.returncode}:\n{done.stdout[-2000:]}\n{done.stderr[-2000:]}"
        )
    return done.stdout


def step(name: str, started: float, **detail: Any) -> None:
    print(
        json.dumps({"step": name, "seconds": round(time.monotonic() - started, 1), **detail}),
        flush=True,
    )


def ingest() -> None:
    t = time.monotonic()
    hours = window()
    with ThreadPoolExecutor(settings.CONCURRENCY) as pool:
        list(
            pool.map(
                lambda h: sh([PY, "-m", "ghflow.commands.ingest", "run", h.key], f"ingest {h.key}"),
                hours,
            )
        )
    step("ingest", t, hours=len(hours))
    t = time.monotonic()
    sh(
        [DBT, "build", "--full-refresh", "--project-dir", PROJECT, "--profiles-dir", PROJECT],
        "dbt build",
    )
    step("dbt build --full-refresh", t)


def judge() -> None:
    t = time.monotonic()
    sh([PY, "-m", "ghflow.commands.quality", "judge", "--run-id", "demo"], "quality judge")
    step("quality judge", t)


def stream_closed(last: Hour) -> dict[str, Any]:
    """Compact what the lander closed; how far the stream and ClickHouse have got."""
    sh([PY, "-m", "ghflow.commands.lander", "compact"], "lander compact")
    import psycopg

    from ghflow.commands import clickhouse
    from ghflow.commands.local import clickhouse as client
    from ghflow.commands.local import ops_dsn

    with psycopg.connect(ops_dsn()) as conn:
        compacted = {r[0] for r in conn.execute("select hour from stream_compacted").fetchall()}
    keys = {h.key for h in window()}
    closed_ch = clickhouse.closed_hours(client())
    return {
        "compacted": len(keys & compacted),
        "clickhouse_closed": len(keys & closed_ch),
        "of": len(keys),
    }


def wait_for_stream() -> None:
    t, last = time.monotonic(), window()[-1]
    while True:
        state = stream_closed(last)
        if state["compacted"] == state["of"] == state["clickhouse_closed"]:
            break
        if time.monotonic() - t > settings.DEMO_STREAM_DEADLINE_S:
            raise RuntimeError(f"the stream did not close the window in time: {state}")
        time.sleep(30)
    step("stream closed every hour", t, **state)


def publish() -> None:
    first, last = window()[0].key, window()[-1].key
    t = time.monotonic()
    sh(
        [
            PY,
            "-m",
            "ghflow.commands.reconcile",
            "run",
            "--from",
            first,
            "--to",
            last,
            "--clickhouse",
        ],
        "reconcile",
    )
    step("reconcile (three witnesses)", t)
    for args, what in (
        ([DBT, "build", "--project-dir", PROJECT, "--profiles-dir", PROJECT, "--select", "stg_ops__hour_verdicts+"], "marts"),
        ([PY, "-m", "ghflow.commands.marts", "twin"], "marts twin"),
        ([PY, "-m", "ghflow.commands.marts", "export"], "marts export"),
        ([PY, "-m", "ghflow.commands.dashboard", "render"], "dashboard render"),
    ):  # fmt: skip
        t = time.monotonic()
        sh(args, what)
        step(what, t)


# ------------------------------------------------------------------ acceptance


def _ops(sql: str) -> list[tuple[Any, ...]]:
    import psycopg

    from ghflow.commands.local import ops_dsn

    with psycopg.connect(ops_dsn()) as conn:
        return conn.execute(sql).fetchall()


def _json(args: list[str]) -> dict[str, Any]:
    done = subprocess.run(args, capture_output=True, text=True)
    return {"exit": done.returncode, **json.loads(done.stdout or "{}")}


def accept() -> dict[str, Any]:
    """Each check reads the outcome itself, never the driver's word for it."""
    keys = {h.key for h in window()}
    final = dict(
        _ops(
            "select distinct on (hour) hour, outcome from ingest_audit order by hour, attempted_at desc"
        )
    )
    verdicts = _ops(
        "select day, status from (select distinct on (hour_start) hour_start::date as day, status from quality_verdicts order by hour_start, judged_at desc) v"
    )
    flagged: dict[str, int] = {}
    for day, status in verdicts:
        flagged[day.isoformat()] = flagged.get(day.isoformat(), 0) + (status == "flagged")
    rec = dict(_ops("select distinct on (hour) hour, status || ':' || coalesce(clickhouse_ids, -1)::text "
                    "from reconcile_results order by hour, checked_at desc"))  # fmt: skip
    broken, clean = settings.DEMO_BROKEN_DAY.isoformat(), settings.DEMO_CLEAN_DAY.isoformat()
    twin = _json([PY, "-m", "ghflow.commands.marts", "twin"])
    dash = _json(
        [
            PY,
            "-m",
            "ghflow.commands.dashboard",
            "check",
            "--site",
            "/opt/ghflow/dashboard-site/current",
        ]
    )
    diff = _json([PY, "-m", "ghflow.commands.repo_state", "diff"])
    checks = {
        "every hour ingested and accepted": all(final.get(k) == "accepted" for k in keys),
        "the broken day is caught (>= DEMO_BROKEN_MIN_FLAGGED hours)": flagged.get(broken, 0) >= settings.DEMO_BROKEN_MIN_FLAGGED,
        "the clean day has no flagged hour": clean in flagged and flagged[clean] == 0,
        "batch = stream = ClickHouse for every hour": all(rec.get(k, "").startswith("green:") and not rec[k].endswith(":-1") for k in keys),
        "every KPI headline matches its twin": bool(twin.get("all_match")),
        "the dashboard reads every number back": bool(dash.get("passed")),
        "the repo-state topic equals dim_repo": diff.get("verdict") == "green",
    }  # fmt: skip
    return {"window": [settings.DEMO_FIRST.isoformat(), settings.DEMO_LAST.isoformat()], "hours": len(keys),
            "flagged_hours_by_day": flagged, "twin": twin.get("kpis"), "dashboard": {k: dash.get(k) for k in ("pages", "numbers_and_names", "charts", "problems")},
            "repo_diff": diff.get("counts"), "checks": checks, "passed": all(checks.values())}  # fmt: skip


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if args not in (["run"], ["accept"]):
        print(__doc__, file=sys.stderr)
        return ExitCode.REFUSED

    def go() -> int:
        if args == ["accept"]:
            record = accept()
            print(json.dumps(record, indent=2, default=str))
            return ExitCode.PASSED if record["passed"] else ExitCode.REFUSED
        started = time.monotonic()
        ingest()
        judge()
        wait_for_stream()
        publish()
        step(
            "demo",
            started,
            window=[settings.DEMO_FIRST.isoformat(), settings.DEMO_LAST.isoformat()],
        )
        return ExitCode.PASSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

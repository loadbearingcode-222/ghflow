"""The insight marts: headlines, their independent twins, and the snapshot export (M5-S1).

    python -m ghflow.commands.marts headlines   # each KPI's headline, read from its mart
    python -m ghflow.commands.marts twin        # ... recomputed from silver + ghflow_ops, compared
    python -m ghflow.commands.marts export      # every mart -> s3://marts/latest/*.parquet + manifest

The twin never reads a mart, staging-based intermediate or dbt's verdict model: it unions the
silver views, reads the verdicts straight from Postgres, and computes each headline its own way.
The export refuses, writing nothing, if any mart is empty (docs/KPIS.md, ADR-0012).
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ghflow.commands.local import ops_dsn, s3_config
from ghflow.exitcodes import ExitCode, guarded
from ghflow.storage import Store

WAREHOUSE = os.environ.get("GHFLOW_WAREHOUSE", "/opt/ghflow/warehouse/ghflow.duckdb")
MARTS = ["mart_daily_activity", "mart_event_mix", "mart_hour_of_week", "mart_rising_repos",
         "mart_bot_share", "mart_top_bots", "mart_trust"]  # fmt: skip
BUCKET = "marts"

#: KPI id -> SQL over the marts returning (value, label).
HEADLINES = {
    "K01": "select median(events_per_day_clean), null from ghflow.main_marts.mart_daily_activity",
    "K02": "select sum(events) filter (where type = 'PushEvent') / sum(events), null from ghflow.main_marts.mart_event_mix",
    "K03": "select avg_events, weekday_name || ' ' || hour_utc || ':00 UTC' from ghflow.main_marts.mart_hour_of_week order by avg_events desc, weekday, hour_utc limit 1",
    "K04": "select stars_recent, repo_name from ghflow.main_marts.mart_rising_repos where rank = 1",
    "K05": "select sum(bot_events) / sum(events), null from ghflow.main_marts.mart_bot_share",
    "K06": "select events, bot from ghflow.main_marts.mart_top_bots where rank = 1",
    "K07": "select sum(reconciled_green) / sum(hours_ingested), null from ghflow.main_marts.mart_trust",
}  # fmt: skip


def connect() -> Any:
    import duckdb  # the dbt group's dependency; present wherever the warehouse is

    con = duckdb.connect(config={"memory_limit": "1GB", "preserve_insertion_order": False})
    con.execute(f"attach '{WAREHOUSE}' as ghflow (read_only)")
    return con


def headlines(con: Any) -> dict[str, list[Any]]:
    return {kpi: list(con.execute(sql).fetchone()) for kpi, sql in HEADLINES.items()}


# ------------------------------------------------------------------ the twin


def flagged_hours() -> pa.Table:
    """Straight from ghflow_ops: the hours whose latest verdict is flagged."""
    import psycopg

    with psycopg.connect(ops_dsn()) as conn:
        rows = conn.execute(
            "select hour_start from (select distinct on (hour_start) hour_start, status "
            "from quality_verdicts order by hour_start, judged_at desc) v where status = 'flagged'"
        ).fetchall()
    starts = [r[0].astimezone(UTC) for r in rows]
    return pa.table({"d": [s.date() for s in starts], "h": [s.hour for s in starts]})


def reconcile_latest() -> pa.Table:
    """Straight from ghflow_ops: each hour's latest three-witness status."""
    import psycopg

    with psycopg.connect(ops_dsn()) as conn:
        rows = conn.execute(
            "select distinct on (hour) hour, status from reconcile_results order by hour, checked_at desc"
        ).fetchall()
    keys = [r[0].rsplit("-", 1) for r in rows]
    return pa.table({"d": [datetime.fromisoformat(k[0]).date() for k in keys],
                     "h": [int(k[1]) for k in keys], "status": [r[1] for r in rows]})  # fmt: skip


TWIN_SQL = {
    "K01": "select median(round(24.0 * e / h)) from (select event_date, count(*) as e, count(distinct event_hour) as h "
           "from clean semi join days using (event_date) group by event_date)",
    "K02": "select count(*) filter (where type = 'PushEvent') / count(*), null from clean semi join days using (event_date)",
    "K03": "select a, wd || ' ' || hr || ':00 UTC' from (select isodow(event_date) as dow, dayname(event_date) as wd, event_hour as hr, "
           "round(avg(n)) as a from (select event_date, event_hour, count(*) as n from clean group by all) group by all) "
           "order by a desc, dow, hr limit 1",
    "K04": "with w as (select max(event_date) as last from days), s as (select repo_id, count(*) as n from clean, w "
           "where type = 'WatchEvent' and event_date > w.last - 14 and event_date <= w.last group by repo_id "
           "order by n desc, repo_id limit 1) select s.n, (select arg_max(repo_name, created_at) from silver_all a "
           "where a.repo_id = s.repo_id) from s",
    "K05": "select count(*) filter (where actor_login like '%[bot]') / count(*), null from clean semi join days using (event_date)",
    "K06": "select count(*) as n, actor_login from clean where actor_login like '%[bot]' group by actor_login order by n desc, actor_login limit 1",
}  # fmt: skip


def twin(con: Any) -> dict[str, list[Any]]:
    views = [r[0] for r in con.execute(
        "select table_name from information_schema.tables where table_catalog = 'ghflow' "
        "and table_schema = 'main_silver' order by 1").fetchall()]  # fmt: skip
    union = " union all ".join(
        f"select type, created_at, actor_login, repo_id, repo_name, event_date, event_hour "
        f"from ghflow.main_silver.{v}"
        for v in views
    )
    con.execute(f"create or replace temp view silver_all as {union}")
    flagged = flagged_hours()  # noqa: F841 - read by DuckDB through its name
    con.execute("create or replace temp table flagged_t as select * from flagged")
    con.execute("create or replace temp view clean as select * from silver_all s "
                "anti join flagged_t f on f.d = s.event_date and f.h = s.event_hour")  # fmt: skip
    con.execute("create or replace temp table days as select event_date from silver_all "
                "where event_date < current_date group by event_date having count(distinct event_hour) = 24")  # fmt: skip
    out = {kpi: list(con.execute(sql).fetchone()) for kpi, sql in TWIN_SQL.items()}
    out["K01"] = [out["K01"][0], None]
    latest = reconcile_latest()  # noqa: F841 - read by DuckDB through its name
    green, hours = con.execute(
        "select count(*) filter (where l.status = 'green'), count(*) from "
        "(select distinct event_date, event_hour from silver_all) s "
        "left join latest l on l.d = s.event_date and l.h = s.event_hour"
    ).fetchone()
    out["K07"] = [green / hours, None]
    return out


def same(a: Any, b: Any) -> bool:
    if isinstance(a, float) or isinstance(b, float):
        return abs(float(a) - float(b)) <= 1e-9 * max(1.0, abs(float(a)))
    return bool(a == b)


def compare() -> dict[str, Any]:
    con = connect()
    marts, twins = headlines(con), twin(con)
    rows = {k: {"mart": marts[k], "twin": twins[k],
                "match": same(marts[k][0], twins[k][0]) and marts[k][1] == twins[k][1]} for k in HEADLINES}  # fmt: skip
    return {"kpis": rows, "all_match": all(r["match"] for r in rows.values())}


# ------------------------------------------------------------------ the export


def export() -> dict[str, Any]:
    con = connect()
    tables = {
        m: con.execute(f"select * from ghflow.main_marts.{m}").to_arrow_table() for m in MARTS
    }
    empty = [m for m, t in tables.items() if t.num_rows == 0]
    if empty:
        return {"refused": f"empty marts: {empty}; nothing written"}
    store, built = Store(s3_config()), datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    manifest: dict[str, Any] = {"built_at": built, "marts": {}, "headlines": headlines(con)}
    for name, table in tables.items():
        sink = io.BytesIO()
        pq.write_table(table, sink, compression="zstd")
        data = sink.getvalue()
        store.put(f"{BUCKET}/snapshots/{built}/{name}.parquet", data)
        store.put(f"{BUCKET}/latest/{name}.parquet", data)
        manifest["marts"][name] = {
            "rows": table.num_rows,
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    body = json.dumps(manifest, indent=2, default=str).encode()
    store.put(f"{BUCKET}/snapshots/{built}/manifest.json", body)
    store.put(
        f"{BUCKET}/latest/manifest.json", body
    )  # last: a reader never sees a half-written set
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["headlines", "twin", "export"])
    args = parser.parse_args(argv)

    def go() -> int:
        if args.command == "headlines":
            print(json.dumps(headlines(connect()), indent=2, default=str))
            return ExitCode.PASSED
        record = compare() if args.command == "twin" else export()
        print(json.dumps(record, indent=2, default=str))
        ok = record.get("all_match", True) and "refused" not in record
        return ExitCode.PASSED if ok else ExitCode.REFUSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

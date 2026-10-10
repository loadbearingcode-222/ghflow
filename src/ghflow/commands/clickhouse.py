"""ClickHouse: its DDL, and the proofs that it agrees with the other witnesses (M4-S1, ADR-0010).

    python -m ghflow.commands.clickhouse ddl [--check]   # write (or check) config/clickhouse/initdb
    python -m ghflow.commands.clickhouse apply           # run the DDL again (idempotent)
    python -m ghflow.commands.clickhouse status          # rows, consumer progress, closed hours
    python -m ghflow.commands.clickhouse verify          # duplicates under FINAL, views = events
    python -m ghflow.commands.clickhouse repo-diff       # repo_current vs dim_repo (needs the warehouse)

Per-hour event ids join reconcile as the third witness (`ghflow.commands.reconcile`).
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from ghflow import clickhouse, settings
from ghflow.commands.local import REPO_ROOT
from ghflow.commands.local import clickhouse as client
from ghflow.exitcodes import ExitCode, guarded

DDL = REPO_ROOT / "config" / "clickhouse" / "initdb" / "001_ghflow.sql"
DB = clickhouse.DATABASE


def ddl(check_only: bool) -> int:
    if check_only:
        same = DDL.exists() and DDL.read_text(encoding="utf-8") == clickhouse.ddl_file()
        return ExitCode.PASSED if same else ExitCode.REFUSED
    DDL.parent.mkdir(parents=True, exist_ok=True)
    DDL.write_bytes(clickhouse.ddl_file().encode())
    return ExitCode.PASSED


def apply() -> dict[str, Any]:
    ch = client()
    statements = clickhouse.statements() + clickhouse.ops_statements()
    for statement in statements:
        ch.command(statement)
    return {"applied": len(statements)}


def closed_hours(ch: Any) -> set[str]:
    """Hours whose END_OF_HOUR marker is in on every partition: complete in ClickHouse."""
    rows = ch.query(
        f"SELECT hour FROM {DB}.hour_markers GROUP BY hour "
        "HAVING uniqExact(partition) = {n:UInt16}",
        parameters={"n": settings.STREAM_PARTITIONS},
    ).result_rows
    return {r[0] for r in rows}


def hour_ids(ch: Any, hour: str) -> list[int]:
    return [
        r[0]
        for r in ch.query(
            f"SELECT DISTINCT id FROM {DB}.events WHERE hour = {{h:String}}", parameters={"h": hour}
        ).result_rows
    ]


# Whole-table distinct counts do not fit: uniqExact over 147M ids needs more than the 1.5 GiB cap
# (gotcha #56). Every check below runs one day at a time (~2.2M ids) and sums.
DAY = "toDate(created_at) = {d:Date}"


def days(ch: Any) -> list[Any]:
    return [
        r[0]
        for r in ch.query(
            f"SELECT DISTINCT toDate(created_at) AS d FROM {DB}.events ORDER BY d"
        ).result_rows
    ]


def status() -> dict[str, Any]:
    ch = client()
    rows = ch.query(f"SELECT count() FROM {DB}.events").result_rows[0][0]
    return {"events_rows": rows, "days": len(days(ch)), "closed_hours": len(closed_hours(ch)),
            "repo_current_rows": ch.query(f"SELECT count() FROM {DB}.repo_current FINAL").result_rows[0][0]}  # fmt: skip


VIEWS_VS_EVENTS = f"""
SELECT count() FROM (
    SELECT hour_start, type, groupBitmapMerge(ids) AS n FROM {DB}.events_hourly
    WHERE toDate(hour_start) = {{d:Date}} GROUP BY hour_start, type
) AS v FULL OUTER JOIN (
    SELECT toStartOfHour(created_at) AS hour_start, type, uniqExact(id) AS n FROM {DB}.events
    WHERE {DAY} GROUP BY hour_start, type
) AS e USING (hour_start, type)
WHERE v.n != e.n
"""


def verify_day(ch: Any, day: Any) -> dict[str, int]:
    p = {"d": day}
    rows, distinct = ch.query(
        f"SELECT count(), uniqExact(id) FROM {DB}.events WHERE {DAY}", parameters=p
    ).result_rows[0]
    final = ch.query(
        f"SELECT count() FROM {DB}.events FINAL WHERE {DAY}", parameters=p
    ).result_rows[0][0]
    groups = ch.query(f"SELECT count() FROM (SELECT DISTINCT hour_start, type FROM {DB}.events_hourly "
                      "WHERE toDate(hour_start) = {d:Date})", parameters=p).result_rows[0][0]  # fmt: skip
    differing = ch.query(VIEWS_VS_EVENTS, parameters=p).result_rows[0][0]
    return {"rows": rows, "distinct_ids": distinct, "rows_under_final": final, "hourly_groups": groups,
            "differing": differing, "final_off": int(final != distinct)}  # fmt: skip


def verify() -> dict[str, Any]:
    """Duplicates (rows beyond distinct ids) collapse under FINAL; the hourly view equals a
    recomputation from `events`, group for group. Day by day, summed."""
    ch = client()
    per_day = {str(d): verify_day(ch, d) for d in days(ch)}
    total = {k: sum(v[k] for v in per_day.values()) for k in ("rows", "distinct_ids", "rows_under_final",
                                                              "hourly_groups", "differing", "final_off")}  # fmt: skip
    return {"days": len(per_day), **{k: total[k] for k in ("rows", "distinct_ids", "rows_under_final", "hourly_groups")},
            "duplicate_rows": total["rows"] - total["distinct_ids"],
            "days_where_final_differs_from_distinct": total["final_off"],
            "final_collapses_duplicates": total["final_off"] == 0,
            "hourly_groups_differing_from_events": total["differing"],
            "views_equal_events": total["differing"] == 0}  # fmt: skip


def load_clickhouse(con: Any) -> int:
    """ClickHouse's repo_current (FINAL: the latest per repo) into table `topic` (M4-S1)."""
    con.execute(
        "create or replace table topic (repo_id bigint, repo_name varchar, as_of timestamptz, off bigint)"
    )
    read = 0
    with client().query_arrow_stream(
        "SELECT repo_id, repo_name, as_of, toInt64(0) AS off FROM ghflow.repo_current FINAL"
    ) as stream:
        for batch in stream:
            con.execute("insert into topic select * from batch")
            read += batch.num_rows
    return read


def caught_up(group: str, topic: str) -> bool:
    """Is nothing readable left after `group`'s committed offsets? Asked of a read_committed
    reader, not of the high watermark: a transactional topic ends in a commit marker no consumer
    ever commits past, so a caught-up group sits one below the watermark."""
    from confluent_kafka import TopicPartition

    from ghflow.stream import clients, repo_state

    consumer = clients.peek_consumer(group)
    meta = consumer.list_topics(topic, timeout=30).topics[topic].partitions
    committed = consumer.committed([TopicPartition(topic, p) for p in meta], timeout=30)
    consumer.close()
    start = {tp.partition: max(tp.offset, 0) for tp in committed}
    reader = clients.changelog_reader()
    try:
        return next(iter(repo_state.read_to_end(reader, topic, start)), None) is None
    finally:
        reader.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["ddl", "apply", "status", "verify", "repo-diff"])
    parser.add_argument("--check", action="store_true", help="ddl: refuse if the file drifted")
    args = parser.parse_args(argv)

    def go() -> int:
        if args.command == "ddl":
            return ddl(args.check)
        if args.command == "repo-diff":
            from ghflow.commands import repo_state

            if not caught_up(clickhouse.STATE_GROUP, clickhouse.STATE_TOPIC):
                print(
                    json.dumps(
                        {
                            "verdict": "pending",
                            "reason": "ClickHouse has not consumed the whole changelog",
                        }
                    )
                )
                return ExitCode.REFUSED
            record = repo_state.diff(plant=False, load=load_clickhouse)
            print(json.dumps(record, indent=2, default=str))
            return ExitCode.PASSED if record["verdict"] == "green" else ExitCode.REFUSED
        record = {"apply": apply, "status": status, "verify": verify}[args.command]()
        print(json.dumps(record, indent=2, default=str))
        ok = args.command != "verify" or (
            record["final_collapses_duplicates"] and record["views_equal_events"]
        )
        return ExitCode.PASSED if ok else ExitCode.REFUSED

    return guarded(go)


if __name__ == "__main__":
    sys.exit(main())

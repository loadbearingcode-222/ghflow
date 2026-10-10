"""One audit row per ingest attempt, in Postgres `ghflow_ops` (operational state, ADR-0002).

Append-only: a re-run adds a row, it never edits one. An hour's verdict is its latest row. The
table is created idempotently by the first writer, so no migration step has to run first.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import Any

import psycopg
from psycopg import sql

from ghflow.hours import Hour

SCHEMA_SQL = """
create table if not exists ingest_audit (
    id              bigserial primary key,
    hour            text        not null,
    hour_start      timestamptz not null,
    attempted_at    timestamptz not null default now(),
    source          text        not null,
    http_verdict    text,
    etag            text,
    content_length  bigint,
    last_modified   text,
    outcome         text        not null
        check (outcome in ('accepted', 'refused', 'failed', 'crashed')),
    refusal_code    text,
    detail          text,
    rows_in         bigint,
    rows_accepted   bigint,
    rows_rejected   bigint,
    raw_sha256      text,
    bronze_sha256   text,
    sidecar_sha256  text,
    duration_ms     bigint
);
create index if not exists ingest_audit_hour on ingest_audit (hour_start, attempted_at);

-- M1-S4: hours the archive re-published (ETag changed). `audit_table` says which audit table the
-- comparison read: only requests from the real one are processed; a red team uses a copy.
create table if not exists reingest_requests (
    id             bigserial primary key,
    hour           text        not null,
    hour_start     timestamptz not null,
    requested_at   timestamptz not null default now(),
    reason         text        not null,
    recorded_etag  text,
    live_etag      text,
    audit_table    text        not null default 'ingest_audit',
    status         text        not null default 'pending' check (status in ('pending', 'done')),
    done_at        timestamptz
);

-- M1-S4: one row per hour per quality run; each verdict records the bars it was taken under.
create table if not exists quality_verdicts (
    id          bigserial primary key,
    run_id      text        not null,
    judged_at   timestamptz not null default now(),
    hour        text        not null,
    hour_start  timestamptz not null,
    status      text        not null,
    findings    jsonb       not null default '[]',
    bars        jsonb       not null
);
create index if not exists quality_verdicts_hour on quality_verdicts (hour_start, judged_at);
-- M1-S5 adds 'shifted' (#22); replaced idempotently, so tables created in M1-S4 migrate in place.
alter table quality_verdicts drop constraint if exists quality_verdicts_status_check;
alter table quality_verdicts add constraint quality_verdicts_status_check
    check (status in ('clean', 'flagged', 'shifted', 'no_verdict'));
-- reconcile_results (M2-S3; three witnesses M4-S1): here since M5-S4, so the ops schema has one
-- home and a fresh deployment has every table before anything (dbt's mart_trust) reads it.
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
-- the third witness (M4-S1)
alter table reconcile_results add column if not exists clickhouse_ids bigint;
alter table reconcile_results add column if not exists ch_missing_count bigint not null default 0;
alter table reconcile_results add column if not exists ch_extra_count bigint not null default 0;
alter table reconcile_results add column if not exists ch_missing bigint[] not null default '{}';
alter table reconcile_results add column if not exists ch_extra bigint[] not null default '{}';
"""


@dataclass
class Attempt:
    """What one attempt at one hour saw and did. `outcome` is set last."""

    hour: str
    hour_start: Any  # datetime (UTC)
    source: str
    outcome: str = "crashed"
    http_verdict: str | None = None
    etag: str | None = None
    content_length: int | None = None
    last_modified: str | None = None
    refusal_code: str | None = None
    detail: str | None = None
    rows_in: int | None = None
    rows_accepted: int | None = None
    rows_rejected: int | None = None
    raw_sha256: str | None = None
    bronze_sha256: str | None = None
    sidecar_sha256: str | None = None
    duration_ms: int | None = None


COLUMNS = tuple(f.name for f in fields(Attempt))


def connect(dsn: str) -> psycopg.Connection[Any]:
    conn = psycopg.connect(dsn, connect_timeout=10, autocommit=True)
    # Serialized: four first ingests on a fresh database ran this DDL at once and deadlocked
    # (found by the M5-S4 demo from a clean clone, gotcha #58). The lock ends with the transaction.
    with conn.transaction():
        conn.execute("select pg_advisory_xact_lock(hashtext('ghflow_ops schema'))")
        conn.execute(SCHEMA_SQL)
    return conn


def record(conn: psycopg.Connection[Any], attempt: Attempt) -> int:
    """Append the attempt; returns its id."""
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    sql = f"insert into ingest_audit ({', '.join(COLUMNS)}) values ({placeholders}) returning id"
    row = conn.execute(sql.encode(), tuple(asdict(attempt).values())).fetchone()
    assert row is not None
    return int(row[0])


def attempts(conn: psycopg.Connection[Any], hour: str) -> list[dict[str, Any]]:
    """Every attempt at one hour, oldest first."""
    cur = conn.execute(
        f"select id, attempted_at, {', '.join(COLUMNS)} from ingest_audit "
        "where hour = %s order by attempted_at, id".encode(),
        (hour,),
    )
    names = [d.name for d in cur.description or []]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


def final_verdicts(
    conn: psycopg.Connection[Any], first: datetime, end: datetime
) -> dict[datetime, str]:
    """Each archive hour's latest outcome in [first, end): its verdict."""
    cur = conn.execute(
        b"select distinct on (hour_start) hour_start, outcome from ingest_audit "
        b"where source = 'archive' and hour_start >= %s and hour_start < %s "
        b"order by hour_start, attempted_at desc, id desc",
        (first, end),
    )
    return {row[0]: row[1] for row in cur.fetchall()}


def attempt_counts(
    conn: psycopg.Connection[Any], first: datetime, end: datetime, since: datetime
) -> dict[datetime, int]:
    """How many archive attempts each hour in [first, end) had since `since` (duplicates show > 1)."""
    cur = conn.execute(
        b"select hour_start, count(*) from ingest_audit where source = 'archive' "
        b"and hour_start >= %s and hour_start < %s and attempted_at >= %s group by hour_start",
        (first, end, since),
    )
    return {row[0]: int(row[1]) for row in cur.fetchall()}


def totals(conn: psycopg.Connection[Any], first: datetime, end: datetime) -> dict[str, Any]:
    """Bytes, rows and outcomes over the final attempt of every archive hour in [first, end)."""
    cur = conn.execute(
        b"with final as (select distinct on (hour_start) * from ingest_audit "
        b"where source = 'archive' and hour_start >= %s and hour_start < %s "
        b"order by hour_start, attempted_at desc, id desc) "
        b"select count(*), sum(content_length), sum(rows_in), sum(rows_accepted), "
        b"sum(rows_rejected), count(*) filter (where outcome = 'refused'), "
        b"min(attempted_at), max(attempted_at), sum(duration_ms) from final",
        (first, end),
    )
    row = cur.fetchone()
    assert row is not None
    names = ("hours", "gz_bytes", "rows_in", "rows_accepted", "rows_rejected", "refused",
             "first_attempt", "last_attempt", "task_ms")  # fmt: skip
    return dict(zip(names, row, strict=True))


def final_attempt_counts(
    conn: psycopg.Connection[Any], first: datetime, end: datetime
) -> dict[datetime, tuple[int, int]]:
    """Per hour in [first, end): (final attempts, distinct final results).

    A result is the outcome plus the bronze sha256. Several finals with ONE result are re-runs
    (idempotent by design, ADR-0002); more than one result is a conflict.
    """
    cur = conn.execute(
        b"select hour_start, count(*), count(distinct outcome || ':' || coalesce(bronze_sha256, '')) "
        b"from ingest_audit where source = 'archive' and outcome in ('accepted', 'refused') "
        b"and hour_start >= %s and hour_start < %s group by hour_start",
        (first, end),
    )
    return {row[0]: (int(row[1]), int(row[2])) for row in cur.fetchall()}


def recorded_etags(
    conn: psycopg.Connection[Any], first: datetime, end: datetime, table: str = "ingest_audit"
) -> dict[datetime, str | None]:
    """The ETag of each archive hour's latest accepted attempt in [first, end), from `table`."""
    query = sql.SQL(
        "select distinct on (hour_start) hour_start, etag from {} where source = 'archive' "
        "and outcome = 'accepted' and hour_start >= %s and hour_start < %s "
        "order by hour_start, attempted_at desc, id desc"
    ).format(sql.Identifier(table))
    return {row[0]: row[1] for row in conn.execute(query, (first, end)).fetchall()}


def request_reingest(
    conn: psycopg.Connection[Any], hour: Hour, recorded: str | None, live: str | None, table: str
) -> int:
    """Queue one hour for re-ingest because its ETag changed; returns the request id."""
    row = conn.execute(
        b"insert into reingest_requests (hour, hour_start, reason, recorded_etag, live_etag, audit_table) "
        b"values (%s, %s, 'etag_changed', %s, %s, %s) returning id",
        (hour.key, hour.start, recorded, live, table),
    ).fetchone()
    assert row is not None
    return int(row[0])


def pending_reingests(conn: psycopg.Connection[Any]) -> list[tuple[int, str]]:
    """Requests from the REAL audit table still to do, oldest hour first."""
    cur = conn.execute(
        b"select id, hour from reingest_requests where status = 'pending' "
        b"and audit_table = 'ingest_audit' order by hour_start, id"
    )
    return [(int(r[0]), r[1]) for r in cur.fetchall()]


def mark_reingested(conn: psycopg.Connection[Any], request_id: int) -> None:
    conn.execute(
        b"update reingest_requests set status = 'done', done_at = now() where id = %s",
        (request_id,),
    )


def record_verdicts(
    conn: psycopg.Connection[Any],
    run_id: str,
    rows: list[tuple[str, datetime, str, str]],
    bars: str,
) -> None:
    """(hour, hour_start, status, findings json) per hour, all under one run id and one set of bars."""
    with conn.cursor() as cur:
        cur.executemany(
            b"insert into quality_verdicts (run_id, hour, hour_start, status, findings, bars) "
            b"values (%s, %s, %s, %s, %s, %s)",
            [(run_id, h, start, status, findings, bars) for h, start, status, findings in rows],
        )

"""One audit row per ingest attempt, in Postgres `ghflow_ops` (operational state, ADR-0002).

Append-only: a re-run adds a row, it never edits one. An hour's verdict is its latest row. The
table is created idempotently by the first writer, so no migration step has to run first.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from typing import Any

import psycopg

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

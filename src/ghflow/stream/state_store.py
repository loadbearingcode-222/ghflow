"""The repo-state processor's store: repo id -> current name, plus the changelog position the
state reflects (M3-S2, ADR-0009; persisted since M3-S3)."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def micros(moment: datetime) -> int:
    return (moment - EPOCH) // timedelta(microseconds=1)


@dataclass(frozen=True)
class Stored:
    name: str
    as_of: int  # micros
    event_id: int


class StateStore:
    """repo id -> current name, in SQLite, plus the changelog position (per partition, the next
    offset) the state reflects. Both are written in one SQLite transaction, after the Kafka
    commit, so the position can lag the changelog (re-reading committed records rewrites the
    same values) but never lead it. WAL: a killed process leaves the last committed state
    (M3-S3, R1; ADR-0009 as amended)."""

    def __init__(self, path: Path, fresh: bool = False) -> None:
        if fresh:
            for leftover in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
                leftover.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.db = sqlite3.connect(path)
        self.db.executescript(
            "pragma journal_mode = wal; pragma synchronous = normal; "
            "create table if not exists state (repo_id integer primary key, name text not null, "
            "as_of integer not null, event_id integer not null); "
            "create table if not exists position (partition integer primary key, "
            "next_offset integer not null);"
        )

    def lookup(self, repo_ids: Iterable[int]) -> dict[int, Stored]:
        ids = json.dumps(list(repo_ids))
        rows = self.db.execute(
            "select repo_id, name, as_of, event_id from state "
            "where repo_id in (select value from json_each(?))",
            (ids,),
        )
        return {r[0]: Stored(r[1], r[2], r[3]) for r in rows}

    def put(
        self, records: Iterable[Mapping[str, Any]], positions: Mapping[int, int] | None = None
    ) -> None:
        """The records and (optionally) the changelog position they bring the store to, atomically."""
        with self.db:
            self.db.executemany(
                "insert or replace into state values (?, ?, ?, ?)",
                [
                    (r["repo_id"], r["repo_name"], micros(r["as_of"]), r["event_id"])
                    for r in records
                ],
            )
            self.db.executemany(
                "insert or replace into position values (?, ?)", sorted((positions or {}).items())
            )

    def positions(self) -> dict[int, int]:
        return dict(self.db.execute("select partition, next_offset from position").fetchall())

    def count(self) -> int:
        return int(self.db.execute("select count(*) from state").fetchone()[0])

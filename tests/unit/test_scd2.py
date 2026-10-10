"""The SCD2 macros (M3-S1), rendered as dbt renders them and run on synthetic events in DuckDB.

Synthetic ids and names only: no real user data. The full-window proof that arrival order does
not matter runs in the stack (`python -m ghflow.commands.history late-hour`).
"""

from __future__ import annotations

import random
from pathlib import Path
from typing import Any

import duckdb
import jinja2
import pytest

MACROS = Path(__file__).resolve().parents[2] / "dbt" / "ghflow" / "macros"

#: (event id, created_at, repo id, repo name)
EVENTS = [
    (1, "2026-08-03 00:00:01", 10, "o/x"),
    (2, "2026-08-03 00:00:05", 10, "o/x"),
    (3, "2026-08-03 01:00:00", 11, "o/a"),
    (4, "2026-08-03 02:00:00", 11, "o/b"),
    (5, "2026-08-03 03:00:00", 11, "o/a"),  # same second as 6: the higher event id wins
    (6, "2026-08-03 03:00:00", 11, "o/c"),
    (7, "2026-08-03 04:00:00", 11, "o/c"),
    (8, "2026-08-03 05:00:00", 11, "o/a"),  # back to an earlier name: a new version
    (9, "2026-08-03 06:00:00", 12, None),  # a null name is not a version
]


def render(macro_file: str, call: str, **context: Any) -> str:
    text = (MACROS / macro_file).read_text(encoding="utf-8")
    text = text.replace("{% test ", "{% macro ").replace("{% endtest %}", "{% endmacro %}")
    return str(jinja2.Template(text + "{{ " + call + " }}").render(**context))


def dimension(incremental: bool = False) -> str:
    body = render(
        "scd2_from_events.sql",
        "scd2_from_events('repo_id', 'repo_name')",
        ref=lambda _: "stg",
        is_incremental=lambda: incremental,
        var=lambda name: {"lookback_days": 2}[name],
    )
    return (
        "select key_id as repo_id, name as repo_name, valid_from, valid_to, "
        f"valid_to is null as is_current from ({body})"
    )


def load(events: list[tuple[int, str, int, str | None]]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("set timezone = 'UTC'")
    con.execute(
        "create table stg (id bigint, created_at timestamptz, repo_id bigint, repo_name varchar)"
    )
    con.executemany("insert into stg values (?, ?::timestamptz, ?, ?)", events)
    con.execute("alter table stg add column event_date date")
    con.execute("update stg set event_date = cast(created_at as date)")
    con.execute(f"create table dim as {dimension()}")
    return con


def versions(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    return con.execute(
        "select repo_id, repo_name, strftime(valid_from, '%H:%M:%S'), "
        "strftime(valid_to, '%H:%M:%S'), is_current from dim order by repo_id, valid_from"
    ).fetchall()


def violations(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    sql = render("scd2_invariants.sql", "scd2_invariants('dim', 'repo_id')")
    return con.execute(sql).fetchall()


def test_islands_ties_and_flip_backs() -> None:
    assert versions(load(EVENTS)) == [
        (10, "o/x", "00:00:01", None, True),
        (11, "o/a", "01:00:00", "02:00:00", False),
        (11, "o/b", "02:00:00", "03:00:00", False),
        (11, "o/c", "03:00:00", "05:00:00", False),
        (11, "o/a", "05:00:00", None, True),
    ]


def test_the_invariants_hold() -> None:
    assert violations(load(EVENTS)) == []


@pytest.mark.parametrize("seed", range(5))
def test_arrival_order_does_not_change_the_dimension(seed: int) -> None:
    shuffled = EVENTS[:]
    random.Random(seed).shuffle(shuffled)
    assert versions(load(shuffled)) == versions(load(EVENTS))


def test_the_invariants_name_a_planted_overlap() -> None:
    """Red team: one version stretched past the next one's start is named, and only it."""
    con = load(EVENTS)
    con.execute(
        "update dim set valid_to = valid_to + interval 1 minute "
        "where repo_id = 11 and repo_name = 'o/b'"
    )
    assert {row[0] for row in violations(con)} == {"no_overlap_no_gap"}
    assert {row[1] for row in violations(con)} == {11}


def test_the_invariants_name_two_current_rows() -> None:
    con = load(EVENTS)
    con.execute("update dim set valid_to = null, is_current = true where repo_name = 'o/b'")
    assert "one_current_row_per_id" in {row[0] for row in violations(con)}


def test_an_incremental_run_equals_a_full_recompute() -> None:
    """M3-S3: the dimension built on the old days, then the reload window's ids recomputed over
    their whole history (delete + insert by id), equals the dimension built on everything."""
    old_days = [(n, f"2026-08-0{d} 10:00:0{n % 10}", repo, name)
                for n, (d, repo, name) in enumerate([(1, 20, "o/p"), (1, 21, "o/q"), (2, 20, "o/r"),
                                                     (2, 22, "o/s")], start=100)]  # fmt: skip
    new_days = [(200, "2026-08-05 09:00:00", 20, "o/p"), (201, "2026-08-06 11:00:00", 23, "o/t"),
                (202, "2026-08-06 12:00:00", 21, "o/q2")]  # fmt: skip
    everything = load(EVENTS + old_days + new_days)

    con = load(EVENTS + old_days)
    con.executemany("insert into stg values (?, ?::timestamptz, ?, ?, null)", new_days)
    con.execute("update stg set event_date = cast(created_at as date)")
    con.execute(f"create table recomputed as {dimension(incremental=True)}")
    touched = {r[0] for r in con.execute("select distinct repo_id from recomputed").fetchall()}
    con.execute("delete from dim where repo_id in (select repo_id from recomputed)")
    con.execute("insert into dim select * from recomputed")

    assert versions(con) == versions(everything)
    assert 22 not in touched and {20, 21, 23} <= touched  # only the reload window's ids
    assert violations(con) == []

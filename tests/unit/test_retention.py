"""Raw retention picks exactly the files older than the keep period, by each file's own hour."""

from __future__ import annotations

from datetime import UTC, datetime

from ghflow import retention

AS_OF = datetime(2026, 10, 5, 0, tzinfo=UTC)  # cutoff = 2026-09-21 00:00


def raw(key: str) -> str:
    return f"raw/gharchive/date={key[:10]}/{key}.json.gz"


def test_the_boundary_hour_is_kept_and_the_one_before_expires() -> None:
    planned = retention.plan([raw("2026-09-21-0"), raw("2026-09-20-23")], AS_OF)
    assert planned.expired == [raw("2026-09-20-23")]
    assert planned.kept == [raw("2026-09-21-0")]


def test_age_is_the_hours_own_time_not_when_it_was_written() -> None:
    # A backfill writes August today; August is still older than 14 days.
    assert retention.plan([raw("2026-08-03-0")], AS_OF).expired == [raw("2026-08-03-0")]


def test_anything_that_is_not_an_hour_file_is_reported_never_deleted() -> None:
    odd = [
        "raw/gharchive/date=2026-08-03/notes.txt",
        "raw/_integration/x.parquet",
        "raw/gharchive/x.json.gz",
    ]
    planned = retention.plan(odd, AS_OF)
    assert planned.expired == [] and sorted(planned.unrecognised) == sorted(odd)


def test_bronze_paths_are_never_candidates() -> None:
    path = "bronze/gharchive/date=2026-08-03/hour=00/events.parquet"
    assert retention.plan([path], AS_OF).expired == []

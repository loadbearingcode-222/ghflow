"""Hour naming and probe verdicts: a request that failed must never read as 'not published'."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from ghflow.archive import Hour, Probe, Verdict, classify, hours_between, hours_of_days, sweep


def test_hour_keys_follow_gh_archive_naming() -> None:
    assert Hour.parse("2026-09-05-5").key == "2026-09-05-5"
    assert Hour.of(date(2026, 9, 5), 15).url == "https://data.gharchive.org/2026-09-05-15.json.gz"
    assert Hour.parse("2026-09-05-0").start == datetime(2026, 9, 5, 0, tzinfo=UTC)


@pytest.mark.parametrize("key", ["2026-09-05-24", "2026-09-05-05x", "2026-09-05", "garbage"])
def test_malformed_hour_keys_are_refused(key: str) -> None:
    with pytest.raises(ValueError):
        Hour.parse(key)


def test_a_naive_or_off_hour_datetime_is_refused() -> None:
    with pytest.raises(ValueError):
        Hour(datetime(2026, 9, 5, 15))
    with pytest.raises(ValueError):
        Hour(datetime(2026, 9, 5, 15, 30, tzinfo=UTC))


def test_hour_ranges_are_inclusive_and_cross_days() -> None:
    hours = hours_between(Hour.parse("2026-09-05-22"), Hour.parse("2026-09-06-1"))
    assert [h.key for h in hours] == [
        "2026-09-05-22",
        "2026-09-05-23",
        "2026-09-06-0",
        "2026-09-06-1",
    ]
    assert len(hours_of_days(date(2026, 8, 3), date(2026, 8, 4))) == 48


@pytest.mark.parametrize(
    ("status", "verdict"),
    [
        (200, Verdict.PUBLISHED),
        (404, Verdict.NOT_PUBLISHED),
        (403, Verdict.REQUEST_FAILED),  # Cloudflare's block is not an absence
        (503, Verdict.REQUEST_FAILED),
        (None, Verdict.REQUEST_FAILED),  # a timeout has no status at all
    ],
)
def test_only_a_404_means_not_published(status: int | None, verdict: Verdict) -> None:
    assert classify(status) is verdict


H = Hour.parse("2026-09-21-7")


def test_a_planted_timeout_is_request_failed_never_not_published() -> None:
    """The red team: a fetcher that always times out."""
    calls: list[str] = []

    def always_times_out(hour: Hour) -> Probe:
        calls.append(hour.key)
        return Probe(hour, Verdict.REQUEST_FAILED, None, detail="TimeoutError")

    [probe] = sweep([H], always_times_out, retries=3, sleep=lambda _: None)
    assert probe.verdict is Verdict.REQUEST_FAILED
    assert probe.attempts == 4
    assert calls == [H.key] * 4


def test_a_timeout_that_recovers_on_retry_is_published() -> None:
    answers = iter([None, None, 200])

    def flaky(hour: Hour) -> Probe:
        status = next(answers)
        return Probe(hour, classify(status), status, size=20_000_000 if status else None)

    [probe] = sweep([H], flaky, retries=3, sleep=lambda _: None)
    assert (probe.verdict, probe.attempts, probe.size) == (Verdict.PUBLISHED, 3, 20_000_000)


def test_a_real_404_is_not_retried() -> None:
    calls: list[str] = []

    def missing(hour: Hour) -> Probe:
        calls.append(hour.key)
        return Probe(hour, classify(404), 404)

    [probe] = sweep([H], missing, retries=3, sleep=lambda _: None)
    assert probe.verdict is Verdict.NOT_PUBLISHED
    assert len(calls) == 1

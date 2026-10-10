"""The backfill's verdict report: every expected hour has exactly one final verdict, or it says which."""

from __future__ import annotations

from ghflow.commands.backfill import verdict_report
from ghflow.hours import Hour, hours_between

HOURS = hours_between(Hour.parse("2026-08-03-0"), Hour.parse("2026-08-03-3"))


def test_a_complete_window() -> None:
    verdicts = {h.start: "accepted" for h in HOURS} | {HOURS[1].start: "refused"}
    ones = {h.start: 1 for h in HOURS}
    report = verdict_report(HOURS, verdicts, ones, {h.start: (1, 1) for h in HOURS})
    assert report["complete"] and report["hours_with_final_verdict"] == 4
    assert report["outcomes"] == {"accepted": 3, "refused": 1}
    assert report["conflicting"] == {} and report["rerun"] == {} and report["retried"] == {}


def test_missing_failed_and_duplicated_hours_are_named() -> None:
    verdicts = {HOURS[0].start: "accepted", HOURS[1].start: "failed", HOURS[2].start: "accepted"}
    attempts = {HOURS[0].start: 2, HOURS[1].start: 1, HOURS[2].start: 2}
    finals = {HOURS[0].start: (1, 1), HOURS[2].start: (2, 2)}
    report = verdict_report(HOURS, verdicts, attempts, finals)
    assert not report["complete"]
    assert report["missing"] == [HOURS[3].key]
    assert report["not_final"] == {HOURS[1].key: "failed"}
    assert report["conflicting"] == {HOURS[2].key: 2}
    assert report["retried"] == {HOURS[0].key: 1, HOURS[1].key: 1}


def test_retries_and_identical_reruns_are_complete_but_a_conflict_is_not() -> None:
    verdicts = {h.start: "accepted" for h in HOURS}
    ones = {h.start: 1 for h in HOURS}
    single = {h.start: (1, 1) for h in HOURS}
    twice = ones | {HOURS[0].start: 2}
    retried = verdict_report(HOURS, verdicts, twice, single)
    assert retried["complete"] and retried["retried"] == {HOURS[0].key: 1}
    rerun = verdict_report(HOURS, verdicts, twice, single | {HOURS[0].start: (2, 1)})
    assert rerun["complete"] and rerun["rerun"] == {HOURS[0].key: 2}
    conflict = verdict_report(HOURS, verdicts, twice, single | {HOURS[0].start: (2, 2)})
    assert not conflict["complete"] and conflict["conflicting"] == {HOURS[0].key: 2}

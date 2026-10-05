"""The backfill's verdict report: every expected hour has exactly one final verdict, or it says which."""

from __future__ import annotations

from ghflow.commands.backfill import verdict_report
from ghflow.hours import Hour, hours_between

HOURS = hours_between(Hour.parse("2026-08-03-0"), Hour.parse("2026-08-03-3"))


def test_a_complete_window() -> None:
    verdicts = {h.start: "accepted" for h in HOURS} | {HOURS[1].start: "refused"}
    report = verdict_report(HOURS, verdicts, {h.start: 1 for h in HOURS})
    assert report["complete"] and report["hours_with_final_verdict"] == 4
    assert report["outcomes"] == {"accepted": 3, "refused": 1}
    assert report["attempted_more_than_once"] == {}


def test_missing_failed_and_duplicated_hours_are_named() -> None:
    verdicts = {HOURS[0].start: "accepted", HOURS[1].start: "failed", HOURS[2].start: "accepted"}
    report = verdict_report(HOURS, verdicts, {HOURS[2].start: 2})
    assert not report["complete"]
    assert report["missing"] == [HOURS[3].key]
    assert report["not_final"] == {HOURS[1].key: "failed"}
    assert report["attempted_more_than_once"] == {HOURS[2].key: 2}

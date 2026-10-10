"""Batch against stream by event id: green, red with every id named, or pending."""

from __future__ import annotations

from ghflow.reconcile import compare


def test_equal_sets_are_green_and_duplicates_are_only_reported() -> None:
    check = compare("2026-08-03-1", [1, 2, 3], [3, 2, 1], duplicates=67_950)
    assert (check.status, check.missing, check.extra, check.duplicates) == ("green", [], [], 67_950)


def test_a_dropped_event_is_red_and_named() -> None:
    check = compare("2026-08-03-1", [1, 2, 3], [1, 3])
    assert check.status == "red" and check.missing == [2] and check.missing_count == 1


def test_an_extra_event_is_red_and_named() -> None:
    check = compare("2026-08-03-1", [1, 2], [1, 2, 9])
    assert check.status == "red" and check.extra == [9]


def test_an_hour_the_stream_has_not_closed_is_pending_not_red() -> None:
    assert compare("2026-08-03-1", [1, 2], None).status == "pending"


def test_named_ids_are_capped_but_counts_are_exact() -> None:
    check = compare("h", range(1000), [])
    assert check.missing_count == 1000 and len(check.missing) == 20

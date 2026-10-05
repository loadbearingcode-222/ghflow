"""The profiler's arithmetic and its era classifiers, on synthetic events (no network)."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from ghflow.hours import Hour
from ghflow.profiling import (
    bisect_boundary,
    profile_lines,
    pull_request_is_trimmed,
    push_is_trimmed,
)

HOUR = Hour.parse("2026-10-02-15")


def event(type_: str, payload: dict[str, object], minute: int = 7) -> bytes:
    created = (HOUR.start + timedelta(minutes=minute)).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = {"id": "1", "type": type_, "created_at": created, "payload": payload}
    return (json.dumps(body) + "\n").encode()


def test_profile_counts_events_types_and_payload_share() -> None:
    lines = [
        event("PushEvent", {"ref": "refs/heads/main"}),
        event("PushEvent", {"ref": "refs/heads/dev"}),
        event("WatchEvent", {"action": "started"}),
    ]
    profile = profile_lines(lines, HOUR, gz_bytes=100)
    assert (profile.events, profile.types) == (3, {"PushEvent": 2, "WatchEvent": 1})
    assert profile.raw_bytes == sum(len(line) for line in lines)
    assert 0 < profile.payload_bytes < profile.raw_bytes
    assert profile.outside_hour == 0


def test_events_outside_the_file_hour_are_counted() -> None:
    lines = [
        event("PushEvent", {}, minute=-1),
        event("PushEvent", {}, minute=60),
        event("PushEvent", {}),
    ]
    assert profile_lines(lines, HOUR, gz_bytes=1).outside_hour == 2


def test_push_shape_is_classified_by_commits() -> None:
    old = [event("PushEvent", {"commits": [], "size": 1})] * 3
    new = [event("PushEvent", {"head": "abc"})] * 3
    assert push_is_trimmed(old) is False
    assert push_is_trimmed(new) is True
    assert push_is_trimmed([event("WatchEvent", {})]) is None  # undecidable, never a guess


def test_pull_request_shape_is_classified_by_key_count() -> None:
    big = {f"k{i}": i for i in range(48)}
    small = {f"k{i}": i for i in range(5)}
    assert pull_request_is_trimmed([event("PullRequestEvent", {"pull_request": big})]) is False
    assert pull_request_is_trimmed([event("PullRequestEvent", {"pull_request": small})]) is True
    assert pull_request_is_trimmed([event("PushEvent", {})]) is None


def test_bisection_finds_the_one_change() -> None:
    change = Hour.parse("2025-08-07-13")
    calls: list[Hour] = []

    def is_after(hour: Hour) -> bool:
        calls.append(hour)
        return hour >= change

    last_old, first_new = bisect_boundary(
        Hour.parse("2024-06-15-15"), Hour.parse("2026-10-02-15"), is_after
    )
    assert (last_old.key, first_new.key) == ("2025-08-07-12", "2025-08-07-13")
    assert len(calls) < 20, "bisection should need about log2(hours) probes"


def test_bisection_refuses_wrong_brackets() -> None:
    with pytest.raises(ValueError):
        bisect_boundary(Hour.parse("2024-06-15-15"), Hour.parse("2026-10-02-15"), lambda _: True)


def test_census_counts_every_event_by_shape() -> None:
    from ghflow.profiling import census_lines

    lines = [
        event("PushEvent", {"commits": []}),
        event("PushEvent", {"head": "abc"}),
        event("PullRequestEvent", {"pull_request": {f"k{i}": i for i in range(48)}}),
        event("WatchEvent", {}),
    ]
    assert census_lines(lines) == {
        "push": 2,
        "push_old_shape": 1,
        "pull_request": 1,
        "pull_request_old_shape": 1,
    }

"""Profile arithmetic and rendering for docs/data/gharchive.md. Pure — no network, no files.

The I/O half (sweeping the archive, streaming hours, reading and writing the record) is
`ghflow.commands.profile`. Everything here takes values it is given, so it is tested offline.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from ghflow import settings
from ghflow.events import events, sized
from ghflow.hours import Hour

BEGIN = "<!-- BEGIN GENERATED: .\\make.ps1 profile-render -->"
END = "<!-- END GENERATED -->"

Lines = Iterable[bytes] | Iterable[str]
Classifier = Callable[[Lines], bool | None]


# ------------------------------------------------------------------ one hour, in full


@dataclass
class HourProfile:
    hour: str
    gz_bytes: int
    raw_bytes: int = 0
    payload_bytes: int = 0
    events: int = 0
    outside_hour: int = 0
    types: dict[str, int] = field(default_factory=dict)


def profile_lines(lines: Lines, hour: Hour, gz_bytes: int) -> HourProfile:
    lo = hour.start.strftime("%Y-%m-%dT%H:%M:%SZ")
    hi = hour.plus(1).start.strftime("%Y-%m-%dT%H:%M:%SZ")
    result = HourProfile(hour.key, gz_bytes)
    types: Counter[str] = Counter()
    for size, event in sized(lines):
        result.raw_bytes += size
        result.events += 1
        types[event.get("type")] += 1
        payload = event.get("payload")
        if payload is not None:
            result.payload_bytes += _compact_size(payload)
        if not lo <= event.get("created_at", lo) < hi:
            result.outside_hour += 1
    result.types = dict(types.most_common())
    return result


def _compact_size(value: Any) -> int:
    """A value's size as compact JSON: serialising for weight, never parsing a line."""
    return len(json.dumps(value, separators=(",", ":")))


# ------------------------------------------------------------------ payload eras


def _first_of_type(lines: Lines, type_: str) -> list[dict[str, Any]]:
    """The payloads of up to ERA_SAMPLE events of one type, in file order."""
    found: list[dict[str, Any]] = []
    for event in events(lines):
        if event.get("type") == type_:
            found.append(event.get("payload") or {})
            if len(found) == settings.ERA_SAMPLE:
                break
    return found


def push_is_trimmed(lines: Lines) -> bool | None:
    """True if PushEvent payloads carry no `commits`, False if they do, None if none seen."""
    payloads = _first_of_type(lines, "PushEvent")
    return None if not payloads else not any("commits" in p for p in payloads)


def pull_request_is_trimmed(lines: Lines) -> bool | None:
    """True if `payload.pull_request` is the small object (10 keys or fewer)."""
    payloads = _first_of_type(lines, "PullRequestEvent")
    return None if not payloads else all(len(p.get("pull_request") or {}) <= 10 for p in payloads)


def bisect_boundary(
    before: Hour, after: Hour, is_after: Callable[[Hour], bool]
) -> tuple[Hour, Hour]:
    """Last hour in the old era and first in the new, assuming the change happened once."""
    if is_after(before) or not is_after(after):
        raise ValueError(f"brackets are wrong: {before.key} must be old, {after.key} new")
    lo, hi = before, after
    while (hi.start - lo.start) > timedelta(hours=1):
        mid = lo.plus(int((hi.start - lo.start).total_seconds() // 7200))
        if is_after(mid):
            hi = mid
        else:
            lo = mid
    return lo, hi


def census_lines(lines: Lines) -> dict[str, int]:
    """Every event of both types in an hour, by shape: the bisection's second witness."""
    counts = Counter[str]()
    for event in events(lines):
        payload = event.get("payload") or {}
        if event.get("type") == "PushEvent":
            counts["push"] += 1
            counts["push_old_shape"] += "commits" in payload
        elif event.get("type") == "PullRequestEvent":
            counts["pull_request"] += 1
            counts["pull_request_old_shape"] += len(payload.get("pull_request") or {}) > 10
    keys = ("push", "push_old_shape", "pull_request", "pull_request_old_shape")
    return {k: counts[k] for k in keys}


# ------------------------------------------------------------------ the record


def daily_totals(sizes: Iterable[tuple[Hour, int | None]]) -> list[dict[str, Any]]:
    """Per-day bytes and near-empty hours, from (hour, size) pairs; None = not published."""
    days: dict[date, dict[str, Any]] = {}
    for hour, size in sizes:
        day = days.setdefault(
            hour.day, {"date": hour.day.isoformat(), "gz_bytes": 0, "near_empty_hours": []}
        )
        if size is not None:
            day["gz_bytes"] += size
            if size < settings.NEAR_EMPTY_BYTES:
                day["near_empty_hours"].append(hour.start.hour)
    return [days[d] for d in sorted(days)]


def windows(daily: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    def window(rows: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "first": rows[0]["date"],
            "last": rows[-1]["date"],
            "days": len(rows),
            "gz_bytes": sum(r["gz_bytes"] for r in rows),
        }

    return {
        "last_14_days": window(daily[-14:]),
        "last_30_days": window(daily[-30:]),
        "whole_window": window(daily),
    }


# ------------------------------------------------------------------ the doc block


def _gb(n: int) -> str:
    return f"{n / 1e9:.1f} GB"


def _mb(n: int) -> str:
    return f"{n / 1e6:.1f} MB"


def _volume(record: dict[str, Any]) -> list[str]:
    d, s = record["definitions"], record["sweep"]
    out = [
        "#### Download needed to backfill",
        "",
        "| Window | Days | Download (gzipped) | Per day |",
        "|---|---|---|---|",
    ]
    for name, label in [
        ("last_14_days", "Last 14 days"),
        ("last_30_days", "Last 30 days"),
        ("whole_window", "Whole window"),
    ]:
        w = record["windows"][name]
        out.append(
            f"| {label} ({w['first']} → {w['last']}) | {w['days']} | {_gb(w['gz_bytes'])} | {_gb(w['gz_bytes'] // w['days'])} |"
        )
    v = s["verdicts"]
    out += [
        "",
        f"Probed {s['hours']:,} hours: {v.get('published', 0):,} published, "
        f"{v.get('not_published', 0):,} not published, {v.get('request_failed', 0):,} request failed "
        f"after {d['retries']} sequential retries ({s['retried']:,} needed a retry), at {d['concurrency']} in flight.",
    ]
    return out


def _near_empty(record: dict[str, Any]) -> list[str]:
    out = [
        "",
        "#### Near-empty hours",
        "",
        f"**{record['near_empty_total']:,} hours** under {_mb(record['definitions']['near_empty_bytes'])} gzipped, all served as HTTP 200:",
        "",
        "| Day | Hours (UTC) under the threshold |",
        "|---|---|",
    ]
    for day in record["daily"]:
        if hours := day["near_empty_hours"]:
            out.append(f"| {day['date']} | {len(hours)} of 24: {', '.join(map(str, hours))} |")
    return out


def _sampled(record: dict[str, Any]) -> list[str]:
    out = [
        "",
        "#### Sampled hours, streamed in full",
        "",
        "| Hour (UTC) | .gz | Raw JSON | Events | Payload share | PushEvent share | Events outside the hour |",
        "|---|---|---|---|---|---|---|",
    ]
    for h in record["sampled_hours"]:
        push = h["types"].get("PushEvent", 0) / h["events"] if h["events"] else 0.0
        out.append(
            f"| {h['hour']} | {_mb(h['gz_bytes'])} | {_mb(h['raw_bytes'])} | {h['events']:,} | "
            f"{h['payload_bytes'] / h['raw_bytes']:.0%} | {push:.0%} | {h['outside_hour']:,} |"
        )
    return out


def _drift(record: dict[str, Any]) -> list[str]:
    out = [
        "",
        "#### Payload drift",
        "",
        f"Found by bisection; each hour is judged on its first {record['definitions']['era_sample']} events of the type.",
        "",
        "| Change | Last hour with the old shape | First hour with the new shape |",
        "|---|---|---|",
    ]
    labels = {
        "push_commits_removed": "PushEvent loses `commits`",
        "pull_request_trimmed": "`pull_request` shrinks to 10 keys or fewer",
    }
    for name, b in record["drift"].items():
        out.append(f"| {labels[name]} | {b['last_old_shape']} | {b['first_new_shape']} |")
    out += [
        "",
        "Second witness: every event of both types in the boundary hours, by shape.",
        "",
        "| Hour (UTC) | PushEvents with `commits` | Full `pull_request` objects |",
        "|---|---|---|",
    ]
    for key, c in record["boundary_census"].items():
        out.append(
            f"| {key} | {c['push_old_shape']:,} of {c['push']:,} | "
            f"{c['pull_request_old_shape']:,} of {c['pull_request']:,} |"
        )
    return out


def render(record: dict[str, Any]) -> str:
    head = [
        f"*Measured {record['observed_at']} by `.\\make.ps1 profile`; record: "
        "`automation/runs/m0-s2/profile.json`.*",
        "",
    ]
    return "\n".join(
        head + _volume(record) + _near_empty(record) + _sampled(record) + _drift(record)
    )


def splice(doc: str, block: str) -> str:
    head, sep1, rest = doc.partition(BEGIN)
    _, sep2, tail = rest.partition(END)
    if not (sep1 and sep2):
        raise ValueError("docs/data/gharchive.md has no GENERATED markers")
    return f"{head}{BEGIN}\n{block}\n{END}{tail}"

"""Measure GH Archive and re-derive every number in docs/data/gharchive.md.

This is the doc's twin script: the record it writes (automation/runs/m0-s2/profile.json) is the
only place the doc's numbers come from, and the block between the GENERATED markers in the doc is
rendered from that record. A test fails if the block and the record disagree, or if the doc's
prose carries a number of its own.

    python -m ghflow.profile measure   # network: sweep, stream the sampled hours, bisect drift
    python -m ghflow.profile render    # no network: re-render the doc block from the record
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any

from ghflow import settings
from ghflow.archive import Probe, Verdict, head, open_hour, sweep
from ghflow.hours import Hour, hours_of_days

REPO_ROOT = Path(__file__).resolve().parents[2]
RECORD = REPO_ROOT / "automation" / "runs" / "m0-s2" / "profile.json"
DOC = REPO_ROOT / "docs" / "data" / "gharchive.md"
BEGIN = "<!-- BEGIN GENERATED: python -m ghflow.profile render -->"
END = "<!-- END GENERATED -->"


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


def profile_lines(lines: Iterable[bytes], hour: Hour, gz_bytes: int) -> HourProfile:
    lo = hour.start.strftime("%Y-%m-%dT%H:%M:%SZ")
    hi = hour.plus(1).start.strftime("%Y-%m-%dT%H:%M:%SZ")
    result = HourProfile(hour.key, gz_bytes)
    types: Counter[str] = Counter()
    for line in lines:
        result.raw_bytes += len(line)
        event = json.loads(line)
        result.events += 1
        types[event.get("type")] += 1
        payload = event.get("payload")
        if payload is not None:
            result.payload_bytes += len(json.dumps(payload, separators=(",", ":")))
        created = event.get("created_at", lo)
        if not lo <= created < hi:
            result.outside_hour += 1
    result.types = dict(types.most_common())
    return result


def profile_hour(hour: Hour) -> HourProfile:
    probe = head(hour)
    if probe.verdict is not Verdict.PUBLISHED or probe.size is None:
        raise RuntimeError(f"{hour.key}: {probe.verdict} {probe.detail}")
    with open_hour(hour) as lines:
        return profile_lines(lines, hour, probe.size)


# ------------------------------------------------------------------ payload eras

#: How many events of the type to inspect before calling an hour's shape. Each must agree.
ERA_SAMPLE = 20


def push_is_trimmed(lines: Iterable[bytes]) -> bool | None:
    """True if PushEvent payloads carry no `commits`, False if they do, None if none seen."""
    seen = with_commits = 0
    for line in lines:
        event = json.loads(line)
        if event.get("type") == "PushEvent":
            seen += 1
            with_commits += "commits" in (event.get("payload") or {})
            if seen == ERA_SAMPLE:
                break
    return None if seen == 0 else with_commits == 0


def pull_request_is_trimmed(lines: Iterable[bytes]) -> bool | None:
    """True if `payload.pull_request` is the small object (10 keys or fewer)."""
    seen = small = 0
    for line in lines:
        event = json.loads(line)
        if event.get("type") == "PullRequestEvent":
            pull_request = (event.get("payload") or {}).get("pull_request") or {}
            seen += 1
            small += len(pull_request) <= 10
            if seen == ERA_SAMPLE:
                break
    return None if seen == 0 else small == seen


Classifier = Callable[[Iterable[bytes]], bool | None]

#: An hour with none of the event type (the Sep 2026 outage had no PushEvents for days) is
#: undecided; the probe moves forward hour by hour, at most this far.
ERA_SKIP_LIMIT = 96


def era_after(hour: Hour, classify: Classifier) -> bool:
    """Is `hour` (or the first decidable hour after it) in the new era?"""
    for step in range(ERA_SKIP_LIMIT):
        with open_hour(hour.plus(step)) as lines:
            verdict = classify(lines)
        if verdict is not None:
            return verdict
    raise RuntimeError(f"no decidable hour within {ERA_SKIP_LIMIT}h of {hour.key}")


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


# ------------------------------------------------------------------ the record


def daily_totals(probes: list[Probe]) -> list[dict[str, Any]]:
    days: dict[date, dict[str, Any]] = {}
    for probe in probes:
        day = days.setdefault(
            probe.hour.day,
            {"date": probe.hour.day.isoformat(), "gz_bytes": 0, "near_empty_hours": []},
        )
        if probe.verdict is Verdict.PUBLISHED and probe.size is not None:
            day["gz_bytes"] += probe.size
            if probe.size < settings.NEAR_EMPTY_BYTES:
                day["near_empty_hours"].append(probe.hour.start.hour)
    return [days[d] for d in sorted(days)]


def census_lines(lines: Iterable[bytes]) -> dict[str, int]:
    """Every event of both types in an hour, by shape: the bisection's second witness."""
    counts = Counter[str]()
    for line in lines:
        event = json.loads(line)
        payload = event.get("payload") or {}
        if event.get("type") == "PushEvent":
            counts["push"] += 1
            counts["push_old_shape"] += "commits" in payload
        elif event.get("type") == "PullRequestEvent":
            counts["pull_request"] += 1
            counts["pull_request_old_shape"] += len(payload.get("pull_request") or {}) > 10
    keys = ("push", "push_old_shape", "pull_request", "pull_request_old_shape")
    return {k: counts[k] for k in keys}


def census_hour(hour: Hour) -> dict[str, int]:
    with open_hour(hour) as lines:
        return census_lines(lines)


def windows(daily: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    def window(rows: list[dict[str, Any]]) -> dict[str, Any]:
        total = sum(r["gz_bytes"] for r in rows)
        return {
            "first": rows[0]["date"],
            "last": rows[-1]["date"],
            "days": len(rows),
            "gz_bytes": total,
        }

    return {
        "last_14_days": window(daily[-14:]),
        "last_30_days": window(daily[-30:]),
        "whole_window": window(daily),
    }


def measure() -> dict[str, Any]:
    probes = sweep(hours_of_days(settings.WINDOW_FIRST, settings.WINDOW_LAST))
    verdicts = Counter(p.verdict.value for p in probes)
    daily = daily_totals(probes)
    print(f"swept {len(probes)} hours: {dict(verdicts)}", file=sys.stderr)

    sampled = []
    for key in settings.SAMPLED_HOURS:
        sampled.append(asdict(profile_hour(Hour.parse(key))))
        print(f"profiled {key}", file=sys.stderr)

    drift = {}
    before, after = Hour.parse(settings.DRIFT_BEFORE), Hour.parse(settings.DRIFT_AFTER)
    for name, classify in [
        ("push_commits_removed", push_is_trimmed),
        ("pull_request_trimmed", pull_request_is_trimmed),
    ]:
        last_old, first_new = bisect_boundary(before, after, partial(era_after, classify=classify))
        drift[name] = {"last_old_shape": last_old.key, "first_new_shape": first_new.key}
        print(f"{name}: {last_old.key} -> {first_new.key}", file=sys.stderr)
    census = {
        key: census_hour(Hour.parse(key))
        for key in sorted({h for b in drift.values() for h in b.values()})
    }

    return {
        "story": "M0-S2",
        "observed_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": settings.BASE_URL,
        "definitions": {
            "window_first": settings.WINDOW_FIRST.isoformat(),
            "window_last": settings.WINDOW_LAST.isoformat(),
            "near_empty_bytes": settings.NEAR_EMPTY_BYTES,
            "concurrency": settings.CONCURRENCY,
            "retries": settings.RETRIES,
            "era_sample": ERA_SAMPLE,
        },
        "sweep": {
            "hours": len(probes),
            "verdicts": dict(sorted(verdicts.items())),
            "retried": sum(p.attempts > 1 for p in probes),
        },
        "daily": daily,
        "windows": windows(daily),
        "near_empty_total": sum(len(d["near_empty_hours"]) for d in daily),
        "sampled_hours": sampled,
        "drift": drift,
        "boundary_census": census,
    }


# ------------------------------------------------------------------ the doc block


def _gb(n: int) -> str:
    return f"{n / 1e9:.1f} GB"


def _mb(n: int) -> str:
    return f"{n / 1e6:.1f} MB"


def render(record: dict[str, Any]) -> str:
    d = record["definitions"]
    out = [
        f"*Measured {record['observed_at']} by `python -m ghflow.profile measure`; record: "
        "`automation/runs/m0-s2/profile.json`.*",
        "",
    ]

    out += [
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

    s = record["sweep"]
    v = s["verdicts"]
    out += [
        "",
        f"Probed {s['hours']:,} hours: {v.get('published', 0):,} published, "
        f"{v.get('not_published', 0):,} not published, {v.get('request_failed', 0):,} request failed "
        f"after {d['retries']} sequential retries ({s['retried']:,} needed a retry), at {d['concurrency']} in flight.",
    ]

    out += [
        "",
        "#### Near-empty hours",
        "",
        f"**{record['near_empty_total']:,} hours** under {_mb(d['near_empty_bytes'])} gzipped, all served as HTTP 200:",
        "",
        "| Day | Hours (UTC) under the threshold |",
        "|---|---|",
    ]
    for day in record["daily"]:
        if day["near_empty_hours"]:
            hours = day["near_empty_hours"]
            out.append(f"| {day['date']} | {len(hours)} of 24: {', '.join(map(str, hours))} |")

    out += [
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

    out += [
        "",
        "#### Payload drift",
        "",
        f"Found by bisection; each hour is judged on its first {d['era_sample']} events of the type.",
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
    return "\n".join(out)


def splice(doc: str, block: str) -> str:
    head_, sep1, rest = doc.partition(BEGIN)
    _, sep2, tail = rest.partition(END)
    if not (sep1 and sep2):
        raise ValueError("docs/data/gharchive.md has no GENERATED markers")
    return f"{head_}{BEGIN}\n{block}\n{END}{tail}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["measure", "render"])
    args = parser.parse_args(argv)
    if args.command == "measure":
        RECORD.parent.mkdir(parents=True, exist_ok=True)
        RECORD.write_text(json.dumps(measure(), indent=2) + "\n", encoding="utf-8", newline="\n")
        print(f"wrote {RECORD.relative_to(REPO_ROOT)}")
    record = json.loads(RECORD.read_text(encoding="utf-8"))
    DOC.write_text(
        splice(DOC.read_text(encoding="utf-8"), render(record)), encoding="utf-8", newline="\n"
    )
    print(f"rendered {DOC.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

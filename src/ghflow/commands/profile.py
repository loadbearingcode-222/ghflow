"""Measure GH Archive and re-derive every number in docs/data/gharchive.md.

This is the doc's twin script: the record it writes (automation/runs/m0-s2/profile.json) is the
only place the doc's numbers come from, and the block between the GENERATED markers in the doc is
rendered from that record. A test fails if the block and the record disagree, or if the doc's
prose carries a number of its own. The arithmetic and the rendering are `ghflow.profiling`
(pure); this module is the I/O around them.

    .\\make.ps1 profile          # network: sweep, stream the sampled hours, bisect drift
    .\\make.ps1 profile-render   # no network: re-render the doc block from the record
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import asdict
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

from ghflow import settings
from ghflow.archive import Probe, Verdict, head, open_hour, sweep
from ghflow.exitcodes import ExitCode, guarded
from ghflow.hours import Hour, hours_of_days
from ghflow.profiling import (
    Classifier,
    HourProfile,
    bisect_boundary,
    census_lines,
    daily_totals,
    profile_lines,
    pull_request_is_trimmed,
    push_is_trimmed,
    render,
    splice,
    windows,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
RECORD = REPO_ROOT / "automation" / "runs" / "m0-s2" / "profile.json"
DOC = REPO_ROOT / "docs" / "data" / "gharchive.md"

CLASSIFIERS: dict[str, Classifier] = {
    "push_commits_removed": push_is_trimmed,
    "pull_request_trimmed": pull_request_is_trimmed,
}


def profile_hour(hour: Hour) -> HourProfile:
    probe = head(hour)
    if probe.verdict is not Verdict.PUBLISHED or probe.size is None:
        raise RuntimeError(f"{hour.key}: {probe.verdict} {probe.detail}")
    with open_hour(hour) as lines:
        return profile_lines(lines, hour, probe.size)


def era_after(hour: Hour, classify: Classifier) -> bool:
    """Is `hour` (or the first decidable hour after it) in the new era?"""
    for step in range(settings.ERA_SKIP_LIMIT):
        with open_hour(hour.plus(step)) as lines:
            verdict = classify(lines)
        if verdict is not None:
            return verdict
    raise RuntimeError(f"no decidable hour within {settings.ERA_SKIP_LIMIT}h of {hour.key}")


def census_hour(hour: Hour) -> dict[str, int]:
    with open_hour(hour) as lines:
        return census_lines(lines)


def sizes(probes: list[Probe]) -> list[tuple[Hour, int | None]]:
    """Each probed hour's size, None unless it was published."""
    return [(p.hour, p.size if p.verdict is Verdict.PUBLISHED else None) for p in probes]


def drift() -> dict[str, dict[str, str]]:
    before, after = Hour.parse(settings.DRIFT_BEFORE), Hour.parse(settings.DRIFT_AFTER)
    found = {}
    for name, classify in CLASSIFIERS.items():
        last_old, first_new = bisect_boundary(before, after, partial(era_after, classify=classify))
        found[name] = {"last_old_shape": last_old.key, "first_new_shape": first_new.key}
        print(f"{name}: {last_old.key} -> {first_new.key}", file=sys.stderr)
    return found


def measure() -> dict[str, Any]:
    probes = sweep(hours_of_days(settings.WINDOW_FIRST, settings.WINDOW_LAST))
    verdicts = Counter(p.verdict.value for p in probes)
    daily = daily_totals(sizes(probes))
    print(f"swept {len(probes)} hours: {dict(verdicts)}", file=sys.stderr)
    sampled = [asdict(profile_hour(Hour.parse(key))) for key in settings.SAMPLED_HOURS]
    boundaries = drift()
    boundary_hours = sorted({h for b in boundaries.values() for h in b.values()})
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
            "era_sample": settings.ERA_SAMPLE,
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
        "drift": boundaries,
        "boundary_census": {key: census_hour(Hour.parse(key)) for key in boundary_hours},
    }


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["measure", "render"])
    args = parser.parse_args(argv)

    def run() -> int:
        if args.command == "measure":
            write_text(RECORD, json.dumps(measure(), indent=2) + "\n")
            print(f"wrote {RECORD.relative_to(REPO_ROOT)}")
        record = json.loads(RECORD.read_text(encoding="utf-8"))
        write_text(DOC, splice(DOC.read_text(encoding="utf-8"), render(record)))
        print(f"rendered {DOC.relative_to(REPO_ROOT)}")
        return ExitCode.PASSED

    return guarded(run)


if __name__ == "__main__":
    sys.exit(main())

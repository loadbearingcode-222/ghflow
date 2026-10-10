"""Judge hours against their rolling clean baseline; every finding names what tripped it.

Two checks, each a ratio to the reference median:
- volume: the hour's rows;
- event mix: each event type's share of the hour, for types that make up at least
  QUALITY_MIX_MIN_SHARE of the reference (rarer types are too noisy to judge by share).
The bars (settings.QUALITY_*) were argued from the seed fortnight before any hour was judged
(automation/runs/m1-s4/bars.json).
"""

from __future__ import annotations

from collections.abc import Collection, Iterable
from dataclasses import dataclass, field

from ghflow import settings
from ghflow.hours import Hour
from ghflow.quality.baseline import HourCounts, median_rows, median_share, reference

CHECKS = ("volume", "mix")


@dataclass(frozen=True)
class Finding:
    check: str  # "volume" | "mix"
    subject: str  # "rows", or the event type
    ratio: float  # the hour's value / the reference median
    value: float
    reference: float


@dataclass
class Verdict:
    hour: Hour
    status: str  # "clean" | "flagged" | "no_verdict"
    reference_hours: int
    findings: list[Finding] = field(default_factory=list)


def _outside(ratio: float, low: float, high: float) -> bool:
    return ratio < low or ratio > high


def volume(counts: HourCounts, ref: list[HourCounts]) -> list[Finding]:
    expected = median_rows(ref)
    ratio = counts.rows / expected if expected else 0.0
    if _outside(ratio, settings.QUALITY_VOLUME_LOW, settings.QUALITY_VOLUME_HIGH):
        return [Finding("volume", "rows", round(ratio, 4), counts.rows, expected)]
    return []


def mix(counts: HourCounts, ref: list[HourCounts]) -> list[Finding]:
    types = sorted({t for r in ref for t in r.types})
    findings = []
    for event_type in types:
        expected = median_share(ref, event_type)
        if expected < settings.QUALITY_MIX_MIN_SHARE:
            continue
        ratio = counts.share(event_type) / expected
        if _outside(ratio, settings.QUALITY_MIX_LOW, settings.QUALITY_MIX_HIGH):
            findings.append(
                Finding("mix", event_type, round(ratio, 4), counts.share(event_type), expected)
            )
    return findings


def judge(counts: HourCounts, ref: list[HourCounts], checks: Collection[str] = CHECKS) -> Verdict:
    if len(ref) < settings.QUALITY_MIN_REFERENCE:
        return Verdict(counts.hour, "no_verdict", len(ref))
    findings = (volume(counts, ref) if "volume" in checks else []) + (
        mix(counts, ref) if "mix" in checks else []
    )
    return Verdict(counts.hour, "flagged" if findings else "clean", len(ref), findings)


def judge_all(
    counts: Iterable[HourCounts], seed: Collection[Hour], checks: Collection[str] = CHECKS
) -> list[Verdict]:
    """Every hour in time order; an hour judged clean joins the baseline of the hours after it."""
    history = {c.hour: c for c in counts}
    clean: set[Hour] = set()
    verdicts = []
    for hour in sorted(history):
        verdict = judge(history[hour], reference(hour, history, clean, seed), checks)
        if verdict.status == "clean":
            clean.add(hour)
        verdicts.append(verdict)
    return verdicts

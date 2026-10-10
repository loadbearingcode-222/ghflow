"""What "normal" is for one hour: the same hour of day on its previous clean days (ADR-0004).

The owner's decision (2026-10-10): a rolling clean baseline. An hour is compared with up to
QUALITY_REFERENCE_SIZE earlier hours at the same hour of day that were themselves judged clean,
looking back at most QUALITY_LOOKBACK_DAYS. Hours in the seed fortnight (Aug 3-16, which has no
clean past yet) are compared with the other seed days at the same hour instead. Excluding
flagged hours keeps an outage from becoming the new normal; following clean hours lets a slow
regime change through.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from statistics import median

from ghflow import settings
from ghflow.hours import Hour


@dataclass(frozen=True)
class HourCounts:
    """One hour's rows by event type (from silver)."""

    hour: Hour
    types: Mapping[str, int] = field(default_factory=dict)

    @property
    def rows(self) -> int:
        return sum(self.types.values())

    def share(self, event_type: str) -> float:
        return self.types.get(event_type, 0) / self.rows if self.rows else 0.0


def reference(
    hour: Hour,
    history: Mapping[Hour, HourCounts],
    clean: Collection[Hour],
    seed: Collection[Hour],
) -> list[HourCounts]:
    """The hours `hour` is judged against; fewer than QUALITY_MIN_REFERENCE means no verdict."""
    if hour in seed:
        return [
            history[h]
            for h in sorted(seed)
            if h.start.hour == hour.start.hour and h != hour and h in history
        ]
    found: list[HourCounts] = []
    for days_back in range(1, settings.QUALITY_LOOKBACK_DAYS + 1):
        earlier = hour.plus(-24 * days_back)
        if earlier in clean and earlier in history:
            found.append(history[earlier])
            if len(found) == settings.QUALITY_REFERENCE_SIZE:
                break
    return found


def median_rows(ref: list[HourCounts]) -> float:
    return float(median(r.rows for r in ref))


def median_share(ref: list[HourCounts], event_type: str) -> float:
    return float(median(r.share(event_type) for r in ref))

"""Batch against stream, hour by hour, by event id (M2-S3, ADR-0007). Pure.

The batch path's answer for an hour is its bronze ids; the stream's is the ids in that hour's
compacted file (distinct by construction; how many duplicates compaction removed is reported
separately, from its own record). An hour is green when the two sets are equal, red when an id is
missing or extra (each named), pending while the stream has not closed it yet. From M4-S1 a third
witness (ClickHouse) joins the same comparison.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from ghflow import settings


@dataclass(frozen=True)
class HourCheck:
    hour: str
    status: str  # "green" | "red" | "pending"
    batch_ids: int
    stream_ids: int
    missing: list[int] = field(default_factory=list)  # in batch, not in stream
    extra: list[int] = field(default_factory=list)  # in stream, not in batch
    missing_count: int = 0
    extra_count: int = 0
    duplicates: int | None = None  # removed at compaction: reported, never a failure


def compare(
    hour: str, batch: Iterable[int], stream: Iterable[int] | None, duplicates: int | None = None
) -> HourCheck:
    batch_set = set(batch)
    if stream is None:
        return HourCheck(hour, "pending", len(batch_set), 0, duplicates=duplicates)
    stream_set = set(stream)
    missing, extra = sorted(batch_set - stream_set), sorted(stream_set - batch_set)
    return HourCheck(
        hour,
        "red" if missing or extra else "green",
        len(batch_set),
        len(stream_set),
        missing[: settings.RECONCILE_NAMED_IDS],
        extra[: settings.RECONCILE_NAMED_IDS],
        len(missing),
        len(extra),
        duplicates,
    )

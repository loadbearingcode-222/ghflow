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
    # The third witness (M4-S1): ClickHouse's distinct ids for the hour, against batch's.
    clickhouse_ids: int | None = None
    ch_missing: list[int] = field(default_factory=list)
    ch_extra: list[int] = field(default_factory=list)
    ch_missing_count: int = 0
    ch_extra_count: int = 0


#: ClickHouse is not a witness in this comparison (two-witness runs, as before M4-S1).
NOT_USED: Iterable[int] = ()


def compare(
    hour: str,
    batch: Iterable[int],
    stream: Iterable[int] | None,
    duplicates: int | None = None,
    clickhouse: Iterable[int] | None = NOT_USED,
) -> HourCheck:
    """Green when every witness in use holds exactly batch's ids; pending while a witness has
    not closed the hour (stream: compacted; ClickHouse: markers in on every partition)."""
    batch_set = set(batch)
    named = settings.RECONCILE_NAMED_IDS
    if stream is None or clickhouse is None:
        return HourCheck(hour, "pending", len(batch_set), 0, duplicates=duplicates)
    stream_set = set(stream)
    missing, extra = sorted(batch_set - stream_set), sorted(stream_set - batch_set)
    ch_used = clickhouse is not NOT_USED
    ch_set = set(clickhouse) if ch_used else batch_set
    ch_missing, ch_extra = sorted(batch_set - ch_set), sorted(ch_set - batch_set)
    red = missing or extra or ch_missing or ch_extra
    return HourCheck(
        hour, "red" if red else "green", len(batch_set), len(stream_set),
        missing[:named], extra[:named], len(missing), len(extra), duplicates,
        len(ch_set) if ch_used else None, ch_missing[:named], ch_extra[:named],
        len(ch_missing), len(ch_extra),
    )  # fmt: skip

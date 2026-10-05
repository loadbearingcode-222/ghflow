"""Hour values: one GH Archive file is the events created in one UTC hour. Pure — no I/O."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from ghflow import settings


@dataclass(frozen=True, order=True)
class Hour:
    """One GH Archive file: the events created in [start, start + 1h), UTC."""

    start: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.start.utcoffset() != timedelta(0):
            raise ValueError(f"Hour must be UTC: {self.start!r}")
        if (self.start.minute, self.start.second, self.start.microsecond) != (0, 0, 0):
            raise ValueError(f"Hour must be on the hour: {self.start!r}")

    @classmethod
    def parse(cls, key: str) -> Hour:
        """`2026-09-05-15` (GH Archive's own naming: the hour is not zero-padded)."""
        day, _, hour = key.rpartition("-")
        if not day or not hour.isdigit() or not 0 <= int(hour) <= 23:
            raise ValueError(f"not a GH Archive hour key: {key!r}")
        parsed = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC)
        return cls(parsed + timedelta(hours=int(hour)))

    @classmethod
    def of(cls, day: date, hour: int) -> Hour:
        return cls(datetime(day.year, day.month, day.day, hour, tzinfo=UTC))

    @property
    def key(self) -> str:
        return f"{self.start:%Y-%m-%d}-{self.start.hour}"

    @property
    def url(self) -> str:
        return f"{settings.BASE_URL}/{self.key}.json.gz"

    @property
    def day(self) -> date:
        return self.start.date()

    def plus(self, hours: int) -> Hour:
        return Hour(self.start + timedelta(hours=hours))


def hours_between(first: Hour, last: Hour) -> list[Hour]:
    """Every hour from `first` to `last`, inclusive."""
    if last < first:
        raise ValueError(f"{last.key} is before {first.key}")
    count = int((last.start - first.start).total_seconds() // 3600) + 1
    return [first.plus(n) for n in range(count)]


def hours_of_days(first: date, last: date) -> list[Hour]:
    return hours_between(Hour.of(first, 0), Hour.of(last, 23))

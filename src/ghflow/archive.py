"""GH Archive addressing and HTTP probes.

Three verdicts, never two. "Not published" needs a real 404 from the server; a timeout, a
connection reset, a Cloudflare 403 or a 5xx is "request failed" — we could not see the file,
which says nothing about whether it exists. Collapsing the two made a 32-way sweep report
dozens of missing hours that all existed (gotcha #32).
"""

from __future__ import annotations

import enum
import gzip
import shutil
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

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


class Verdict(enum.StrEnum):
    PUBLISHED = "published"
    NOT_PUBLISHED = "not_published"
    REQUEST_FAILED = "request_failed"


def classify(status: int | None) -> Verdict:
    if status == 200:
        return Verdict.PUBLISHED
    if status == 404:
        return Verdict.NOT_PUBLISHED
    return Verdict.REQUEST_FAILED


@dataclass(frozen=True)
class Probe:
    hour: Hour
    verdict: Verdict
    status: int | None
    size: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    detail: str = ""
    attempts: int = 1


def _request(hour: Hour, method: str) -> urllib.request.Request:
    return urllib.request.Request(
        hour.url, method=method, headers={"User-Agent": settings.USER_AGENT}
    )


def head(hour: Hour, timeout: float = settings.HTTP_TIMEOUT_S) -> Probe:
    try:
        with urllib.request.urlopen(_request(hour, "HEAD"), timeout=timeout) as response:
            length = response.headers.get("Content-Length")
            return Probe(
                hour,
                classify(response.status),
                response.status,
                int(length) if length else None,
                response.headers.get("ETag"),
                response.headers.get("Last-Modified"),
            )
    except urllib.error.HTTPError as exc:
        return Probe(hour, classify(exc.code), exc.code, detail=f"HTTP {exc.code}")
    except (TimeoutError, urllib.error.URLError, OSError) as exc:
        return Probe(hour, Verdict.REQUEST_FAILED, None, detail=type(exc).__name__)


Fetch = Callable[[Hour], Probe]


def sweep(
    hours: Sequence[Hour],
    fetch: Fetch = head,
    *,
    concurrency: int = settings.CONCURRENCY,
    retries: int = settings.RETRIES,
    pause_s: float = settings.RETRY_PAUSE_S,
    sleep: Callable[[float], None] = time.sleep,
) -> list[Probe]:
    """Probe every hour; retry each failed request sequentially before accepting the failure.

    The retry is sequential and slow on purpose: a failure under load is usually the load.
    A probe still failing after `retries` keeps its REQUEST_FAILED verdict — it is never
    promoted to NOT_PUBLISHED.
    """
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        first = list(pool.map(fetch, hours))
    settled: list[Probe] = []
    for probe in first:
        attempts = 1
        while probe.verdict is Verdict.REQUEST_FAILED and attempts <= retries:
            sleep(pause_s * attempts)
            probe = fetch(probe.hour)
            attempts += 1
        settled.append(replace(probe, attempts=attempts))
    return settled


@contextmanager
def open_hour(hour: Hour, timeout: float = settings.HTTP_TIMEOUT_S) -> Iterator[gzip.GzipFile]:
    """Stream one hour's decompressed JSON lines; nothing touches the disk."""
    with (
        urllib.request.urlopen(_request(hour, "GET"), timeout=timeout) as response,
        gzip.GzipFile(fileobj=response) as lines,
    ):
        yield lines


def download_hour(hour: Hour, dest: Path, timeout: float = settings.HTTP_TIMEOUT_S) -> Path:
    """Save one hour's .json.gz exactly as served.

    Streams into `<dest>.part` and renames it into place only when complete; on any failure the
    partial file is removed, so `dest` is either the whole file or absent — never a truncated one.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(dest.suffix + ".part")
    try:
        with (
            urllib.request.urlopen(_request(hour, "GET"), timeout=timeout) as response,
            partial.open("wb") as out,
        ):
            shutil.copyfileobj(response, out, length=1 << 20)
        partial.replace(dest)
    finally:
        partial.unlink(missing_ok=True)
    return dest

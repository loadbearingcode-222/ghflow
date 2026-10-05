"""Which raw files have outlived their keep period. Pure: takes paths and a moment, returns paths.

Only `raw/gharchive/.../<hour>.json.gz` is ever a candidate; anything else under raw (or any
path whose name is not an hour key) is left alone and reported, never guessed at.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from ghflow import settings
from ghflow.bronze import RAW_BUCKET
from ghflow.hours import Hour

PREFIX = f"{RAW_BUCKET}/gharchive/"
SUFFIX = ".json.gz"


@dataclass
class Plan:
    cutoff: datetime
    expired: list[str] = field(default_factory=list)
    kept: list[str] = field(default_factory=list)
    unrecognised: list[str] = field(default_factory=list)


def hour_of(path: str) -> Hour | None:
    if not (path.startswith(PREFIX) and path.endswith(SUFFIX)):
        return None
    try:
        return Hour.parse(path.rpartition("/")[2].removesuffix(SUFFIX))
    except ValueError:
        return None


def plan(paths: Iterable[str], as_of: datetime, keep_days: int = settings.RAW_KEEP_DAYS) -> Plan:
    """Expired: the hour began more than `keep_days` before `as_of`."""
    result = Plan(cutoff=as_of - timedelta(days=keep_days))
    for path in sorted(paths):
        hour = hour_of(path)
        if hour is None:
            result.unrecognised.append(path)
        elif hour.start < result.cutoff:
            result.expired.append(path)
        else:
            result.kept.append(path)
    return result

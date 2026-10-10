"""Which hours the archive re-published after we ingested them: their ETag changed.

An hour is scheduled for re-ingest when the archive now serves it with an ETag different from
the one recorded by its latest accepted ingest. A probe that failed (no ETag) proves nothing and
schedules nothing.
"""

from __future__ import annotations

from collections.abc import Mapping

from ghflow.hours import Hour


def republished(recorded: Mapping[Hour, str | None], live: Mapping[Hour, str | None]) -> list[Hour]:
    """Hours whose live ETag is known and differs from the recorded one, in time order."""
    return sorted(
        h for h, now in live.items() if now is not None and h in recorded and recorded[h] != now
    )

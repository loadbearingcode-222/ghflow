"""Data quality for the batch path (M1-S4, ADR-0004). Pure: counts in, verdicts out.

- `baseline`: the rolling clean reference an hour is judged against.
- `checks`: volume and event-mix checks, each finding naming what tripped it.
- `etag`: which hours the archive re-published since they were ingested.
"""

# 0008: Repo and actor history is SCD2, recomputed in full from the event log
Date: 2026-10-10 · Story: M3-S1 · Status: accepted

## Context

GH Archive has no "rename" event. Each event carries the repo's and the actor's name *as of that
event*, so the history of a name can only be read off the log. Measured first
(`automation/runs/m3-s1/measured.json`), over 147.36M events:
- 121,279 of 13.58M repos (0.89%) carry more than one name;
- 27,498 of 7.11M actors (0.39%) carry more than one name;
- 847 repos flip back to an earlier name.

Renames are common enough to matter (the plan's 20% risk did not happen). Hours can arrive late:
a re-ingest after an ETag change (ADR-0004), or a backfill filling a gap (gotcha #48).

## Decision

1. **A version is an island of consecutive events with the same name.** A → B → A is three
   versions, never merged into two.
2. **One name per (id, second).** GH Archive's `created_at` has second precision. When two names
   share a second, the name of the highest event id wins, so `valid_from` is strictly
   increasing per id.
3. **`valid_to` is the next version's `valid_from`.** Validity has no gaps and no overlaps; the
   version without a `valid_to` is current.
4. **Recomputed in full from staging on every build (`materialized='table'`), not merged
   incrementally.** The dimension is then a pure function of the events present, so the order
   hours arrive in cannot change it. That is the Accept-when, proved by
   `python -m ghflow.commands.history late-hour`. The cost on the whole window is about 30 s for
   repos and 17 s for actors, at about 1.3 GiB RSS.
5. **Only multi-name ids pay for the window functions.** One pass groups events by (id, name),
   and an id with a single pair is one version. The name is a grouping key, not an aggregate
   state: DuckDB spills grouping keys, while string aggregate states ran a 3 GiB container out of
   memory (gotcha #52).
6. **The invariants are a dbt test (`scd2_invariants`), each violation named by its rule:**
   - one current row per id;
   - `valid_from` strictly increasing;
   - no overlap and no gap;
   - each version ends after it starts;
   - current means open.

## Consequences

- An incremental SCD2 (merge only the new hours) would be cheaper, but a late hour would then
  have to rewrite history that is already closed. M3-S3 measures whether the full recompute's
  cost justifies that. Any incremental variant must still pass `late-hour` and equal the full
  table row for row.
- `valid_from` is the first time the archive *saw* a name, not when GitHub renamed it. The real
  rename happened at or before that moment, and the dimension never claims more.
- The current rows are the reference that the repo-state topic (M3-S2) and ClickHouse's
  `repo_current` (M4-S1) must equal.

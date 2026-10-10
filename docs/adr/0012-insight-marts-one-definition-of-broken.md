# 0012: One mart per KPI, one definition of a broken hour, a twin for every headline
Date: 2026-10-10 · Story: M5-S1 · Status: accepted

## Context

The stakeholder dashboard (M5-S2) states numbers to people who can't check them. Each number
must be traceable to one formula, and must agree with the data quality the pipeline already
judged.

## Decision

1. **One dbt mart per KPI** (`models/marts/mart_*`, materialized tables). Each has a `meta.kpi`
   id with its row in `docs/KPIS.md` (formula, source, window, owner). A unit test fails on a
   mart without an entry, or an entry without a mart.
2. **Broken hours are defined once:** `stg_ops__hour_verdicts` is the latest verdict per hour
   from `ghflow_ops.quality_verdicts`.
   - DuckDB attaches Postgres read-only (the postgres extension, baked into the image).
   - Every mart reads trust through it, directly or via `int_hourly_events`.
   - A test refuses a mart that reads the verdict table around the model.
   - `flagged` hours are counted and shown, never averaged, shared or ranked.
3. **A twin for every headline:** `ghflow.commands.marts twin` recomputes each KPI from the
   silver views, with the verdicts read straight from Postgres, never from a mart. Counts must
   be equal; ratios must be equal within 1e-9.
4. **Snapshots, not the warehouse, feed the dashboard:** `export` writes every mart to
   `s3://marts/snapshots/<build>/` and `marts/latest/`, the manifest last. It refuses, writing
   nothing, if any mart is empty.
5. **`dbt_build` = build → compact → twin → export,** all in the `duckdb` pool. A twin mismatch
   stops the export, so the dashboard never shows a number that disagrees with its twin.

## Consequences

- Marts are full recomputes, about 30 s for the window. M5-S3 can make the heavy ones
  incremental, if measured worth it.
- Repos are named and ranked (`mart_rising_repos`) by their current `dim_repo` name. Repo names
  contain owner logins; ranking repos is allowed, and ranking people is not (M5-S2's test).

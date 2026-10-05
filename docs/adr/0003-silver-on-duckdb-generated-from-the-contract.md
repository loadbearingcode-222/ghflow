# 0003: Silver on DuckDB — one staging table, typed views generated from the contract
Date: 2026-10-05 · Story: M1-S3 · Status: accepted

## Context

Bronze holds one Parquet file per hour: 9.65 GB for the 1,524-hour window, 142.9M rows. M1-S3
turns it into typed silver per event type, with dbt contracts enforced. Payload fields change by
era: GitHub trimmed PushEvent and `pull_request` at 2025-10-08 17:00. The contract
(`ghflow.fields`) already says which field exists in which era. Silver must agree with it, and
must never become a second place where eras are declared.

## Decision

1. **One DuckDB file, one writer.** The warehouse is `/opt/ghflow/warehouse/ghflow.duckdb` in the
   `warehouse` volume. Only `dbt_build` writes it, through the one-slot Airflow pool `duckdb`.
   Anything else that needs silver reads exported files or a read-only attach.
2. **Staging is one incremental table** (`main_staging.stg_gharchive__events`), loaded from bronze
   in a single scan. Each run deletes and re-reads the newest `lookback_days` (2) *days*, so a
   re-ingested recent hour flows through. `--full-refresh` rebuilds the window. The key is the
   day alone: dbt-duckdb's `delete+insert` on `(date, hour)` took over 10 minutes for one day.
3. **Silver is one contract-enforced view per known event type**, plus `silver_unknown_events`
   for types the contract does not know. Views over the staging table keep one copy on disk.
   Reading bronze from S3 once per type would mean 17 full scans.
4. **Silver is generated from `ghflow.fields`** (`ghflow.silver`, `python -m
   ghflow.commands.silver generate`): the model SQL, the contract YAML and the per-hour
   rows test. `check` (in a unit test and in CI's `dbt-parse` job) refuses committed files that
   drift from the generator. A field's era is therefore declared once.
5. **Eras are tests with names.** Every payload field with an era gets an `in_era` test named
   `era__<model>__<field>__<from_|until_><hour>`. A value outside its era fails a test whose name
   says which field and which era. The red team (`commands.silver redteam`) plants one in a
   disposable copy of the warehouse.
6. **Rows are accounted per hour.** A generated singular test counts bronze rows from the Parquet
   files themselves and the silver rows across all views. Every hour where they differ is a
   failing row.
7. **No network at build time.** DuckDB's `httpfs` extension is baked into the image
   (`/opt/ghflow/duckdb-extensions`).
8. **Memory is budgeted from a measurement, not from `memory_limit`.** A full build's `dbt`
   process (DuckDB in-process) peaked at about 2.1 GiB RSS. That held at `memory_limit` 1 GB and
   1.5 GB, and at 2 or 4 threads: the limit governs DuckDB's buffer manager, not the Parquet and
   httpfs readers' own allocations. With the scheduler (~0.25 GiB) and an hourly ingest that may
   overlap (~0.33 GiB), the scheduler's `mem_limit` is 4 GB. The stack's total rose from about
   6.4 to 7.4 GB of the 14 GB budget.

## Options considered

- **Silver as one table per type** (materialized copies). Rejected for now: it doubles disk and
  build time for no query that needs it yet. Revisit if gold or the marts read silver hot.
- **Hand-written silver models.** Rejected: the eras would then live in two places (Python and
  YAML) and would drift. A generator plus a drift check costs one CLI.
- **Views straight over S3 bronze** (no staging table). Rejected: every query would re-read S3,
  and the per-type views would multiply it.

## Consequences

- The first build reads all of bronze once; daily builds read about three days.
- A change to `ghflow.fields` (a new field or era) is one edit plus `generate`. CI fails until
  the dbt files match.
- An older re-ingested hour, beyond 2 days, needs `dbt_build` triggered with
  `{"full_refresh": true}`. M1-S4's ETag re-check must remember this.
- Backup: the warehouse regenerates from bronze, so it needs none.

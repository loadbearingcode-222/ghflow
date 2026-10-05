# 0002: Bronze is projected Parquet with a sidecar; every attempt is audited in Postgres
Date: 2026-10-05 · Story: M1-S1 · Status: accepted

## Context

M1-S1 lands the first live hour. Three things have to be decided once, because every later
story builds on them: what bronze holds, where an hour's files live, and where the record of
each attempt goes. The replayed stream (M2) publishes bronze, not raw, so bronze is also the one
parse both transports share.

## Decision

1. **Bronze is projected.** One Parquet file per hour holds the contract's columns
   (`ghflow.fields`): the envelope plus the typed payload fields, never free-text bodies (issue,
   PR and commit text). The raw `.gz` is kept for 14 days (M1-S2 retention). Anything older can be
   fetched from GH Archive again, which is the backup.
2. **A rejected-row sidecar is always written**, even when empty: one Parquet file per hour with
   `line`, `rule` and `raw`. "Rows in = bronze rows + sidecar rows" can then be checked from
   storage alone, without the audit table.
3. **Encoding is deterministic.** It uses a fixed schema, zstd at a fixed level and one row
   group, written to memory. The only metadata is the hour key. The same rows give the same
   bytes, so a re-run proves itself by sha256, and "re-run" means overwrite, never a second
   object.
4. **Paths** are `raw/gharchive/date=D/<hour>.json.gz`,
   `bronze/gharchive/date=D/hour=HH/events.parquet` and
   `bronze/gharchive_rejected/date=D/hour=HH/rejected.parquet`. They are Hive-style, so DuckDB
   (M1-S3) reads `date` and `hour` as columns.
5. **Every attempt appends one row** to `ghflow_ops.ingest_audit`. The row holds the source, the
   HTTP verdict, the ETag, length and Last-Modified of the same GET response, the outcome
   (`accepted` / `refused` / `failed` / `crashed`), the row counts and the three sha256s. It is
   written in a `finally`, so a crash is recorded before it propagates. An hour's verdict is its
   latest row. The table is created idempotently by its first writer.
6. **The data interval is the watermark.** The DAG runs on `CronDataIntervalTimetable`, so run
   `[H, H+1)` ingests file `H` (gotcha #43). There is no stored "last ingested" state.
7. **The DAG stays thin.** Airflow's interpreter never imports ghflow. Each task runs
   `python -m ghflow.commands.ingest` in the project venv and maps its exit code: `1` (refused)
   fails with no retry, because a structure change is not transient; anything else retries.

## Options considered

- **Full raw JSON in bronze (a `payload` string column).** Rejected: it doubles bronze, and free
  text is the riskiest data (personal content) with no planned use. Raw `.gz` covers the 14-day
  replay need.
- **Audit in a Parquet/JSON log in S3.** Rejected: several writers (ingest, retention, quality,
  replayer checkpoints later) need transactional appends and queries by hour. That is Postgres's
  job, and `ghflow_ops` already exists (ADR-0001).
- **Airflow tasks importing ghflow directly** (installed into Airflow's environment).
  Rejected: olist's ADR-0007 showed project dependencies breaking Airflow's own. The isolated
  venv costs one subprocess per task.

## Consequences

- Bronze for the whole backfill is a fraction of raw, about one file per hour plus a small
  sidecar. Measured per hour in M1-S2's backfill record.
- A future field needs a contract change plus a re-derive from raw (≤ 14 days old) or from GH
  Archive. That is acceptable, because every artifact regenerates from the archive plus code.
- Byte identity depends on the pyarrow version. Upgrading pyarrow may change bytes without
  changing data, so a pin bump must re-baseline the bronze sha256s (noted for M1-S5).
- Backup obligations: bronze and raw regenerate from GH Archive, so they need no backup.
  `ingest_audit` is the only record of attempts and would be lost with the `platform-db`
  volume. Its rows can be re-derived only partly, by re-running. Until a backup exists,
  `reset -Force` costs the audit history.

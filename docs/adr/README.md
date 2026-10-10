# Architecture decision records

One file per decision that is hard to reverse or worth arguing: `NNNN-<slug>.md`. Records are
immutable: supersede or amend with a marker, never rewrite.

Template:

```
# NNNN: <decision>
Date: YYYY-MM-DD · Story: M#-S# · Status: accepted | superseded by NNNN

## Context
## Options
## Decision
## Cost (honest)
## Revisit when
```

| ADR | Decision | Story |
|---|---|---|
| [0001](0001-platform-core-and-pins.md) | Platform core: SeaweedFS (MinIO dropped), Postgres, Airflow 3; pins by tag and digest; doctor counts mem_limit | M0-S4 |
| [0002](0002-bronze-projected-and-audited.md) | Bronze is projected Parquet + an always-written sidecar, deterministic bytes, Hive paths; one audit row per attempt in `ghflow_ops`; the data interval is the watermark; thin DAG | M1-S1 |
| [0003](0003-silver-on-duckdb-generated-from-the-contract.md) | Silver on DuckDB: one incremental staging table, typed contract-enforced views per event type generated from `ghflow.fields`, named era tests, one writer (pool `duckdb`) | M1-S3 |
| [0004](0004-quality-against-a-rolling-clean-baseline.md) | Quality against a rolling clean baseline (owner 2026-10-10); bars argued before prediction before run; one `quality_verdicts` table; ETag re-check queues re-ingest; known drift weakness #22 | M1-S4 |
| [0005](0005-replayed-stream-redpanda-avro.md) | Replayed stream: Redpanda + registry, topics as code (never shrink, LogAppendTime), Avro generated from bronze, named rules, BACKWARD twice, end-of-hour markers, checkpoint after flush | M2-S1 |
| [0006](0006-lander-per-partition-hour.md) | Lander: one file per (partition, hour) bounded by markers, commit after write, exactly-once coverage; compaction of closed hours only, content-equal, dedup by id | M2-S2 |
| [0007](0007-reconcile-by-event-id.md) | Batch vs stream reconciled per hour by event id: green / red (ids named) / pending; duplicates reported not failed; results table; disposable red team; ClickHouse joins in M4-S1 | M2-S3 |
| [0008](0008-scd2-recomputed-from-the-event-log.md) | Repo and actor SCD2 recomputed in full from staging: islands of same-name events (A→B→A = 3 versions), one name per (id, second) by highest event id, valid_to = next valid_from; only multi-name ids pay the windows; invariants as a named dbt test; arrival order proved irrelevant | M3-S1 |
| [0009](0009-repo-state-transactional-changelog.md) | Repo state: compacted `gharchive.repo_state` keyed by repo id; one Kafka transaction per closed (partition, hour) with records + offsets; only net changes; SQLite store is a cache rebuilt from the changelog on start; `diff` vs `dim_repo` current over the overlap of both witnesses | M3-S2 |
| [0010](0010-clickhouse-third-witness.md) | ClickHouse reads both topics with its Kafka engine (AvroConfluent, own groups, read_committed); DDL generated from bronze; ReplacingMergeTree by (hour, id), uniqExact hourly states, markers as data; third reconcile witness; 2 GiB, 64k-row blocks | M4-S1 |

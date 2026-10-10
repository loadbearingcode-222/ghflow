# 0010: ClickHouse consumes the topics itself and is the third witness
Date: 2026-10-10 · Story: M4-S1 · Status: accepted

## Context

Grafana (M4-S2) and fast ad-hoc questions need a columnar store fed continuously. ClickHouse can
read Kafka itself, but it brings its own delivery semantics. The project's central claim, that
every path agrees to the event, has to hold for it too.

## Decision

1. **ClickHouse reads the topics with its Kafka engine, not through a ghflow service.**
   - The format is AvroConfluent: the schema comes from the registry by the id in each record.
   - There are two consumer groups of its own: `clickhouse` on `gharchive.events` and
     `clickhouse-repo-state` on `gharchive.repo_state`.
   - It reads committed records only (`isolation_level=read_committed`, since the repo-state
     changelog is transactional, ADR-0009).
2. **The DDL is generated from `bronze.SCHEMA`** (`ghflow.clickhouse`). The committed file
   `config/clickhouse/initdb/001_ghflow.sql` runs on an empty volume; `... clickhouse apply`
   re-applies it idempotently. A unit test refuses drift.
3. **Delivery is at least once, so every table tolerates a re-read block:**
   - `events` is a ReplacingMergeTree ordered by (hour of `created_at`, event id). A duplicate
     sorts onto the same key and collapses at merge time or under `FINAL`.
   - `events_hourly` keeps `groupBitmap(id)` states (exact Roaring bitmaps) per (hour, type),
     never `count()`. A re-read block cannot inflate it, so it must equal a recomputation from
     `events`. The first version used `uniqExact` states: hash sets of every id, about 1.2 GB
     for the window, whose merges starved the server. Bitmaps hold the same ids in 275 MiB
     (gotcha #56).
   - `repo_current` is a ReplacingMergeTree(`as_of`): the latest name per repo.
4. **Markers are data.** END_OF_HOUR records are filtered out of `events` into `hour_markers`.
   An hour is complete in ClickHouse when all 6 partitions' markers are in. Only then is it
   compared.
5. **The third witness:** `reconcile --clickhouse` compares each hour's distinct ids in
   ClickHouse with bronze's, alongside the stream's. An hour is green only when all three agree;
   ClickHouse's mismatches are named like the stream's. The hourly DAG runs with three witnesses.
6. **The budget:** a 2 GiB `mem_limit` and `max_server_memory_usage` of 1.5 GiB. The Kafka block
   is capped at 65,536 rows, because the default of about 1M wide rows pushed through three views
   exceeded the cap (gotcha #55).

## Consequences

- A replay from scratch needs the events topic, which keeps 7 days; after that, ClickHouse is
  rebuilt by replaying bronze, as the stream is.
- Checks of exact distinct counts run per day, never over the whole window (gotcha #56).
- `FINAL` costs a merge at read time. M4-S3 measures panel latency, and may prefer
  `uniqExact(id)` or pre-aggregated views over `FINAL`.

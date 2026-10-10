# 0006: The lander writes one file per (partition, hour), commits after the write, compacts closed hours
Date: 2026-10-10 · Story: M2-S2 · Status: accepted

## Context

The stream must land in S3 so it can be reconciled with the batch path (M2-S3). A consumer that
commits before writing can lose data. One that writes before committing can write twice, and
with time- or count-based batches a restart cuts *different* ranges, so files overlap. Consumers
also need to know when an hour is whole.

## Decision

1. **File boundaries come from the data, not the clock.** The replayer publishes hours in order,
   each ended by one END_OF_HOUR marker per partition (ADR-0005). The lander buffers a partition
   until that hour's marker arrives. The file is exactly `[first buffered offset, marker offset]`,
   named by that range: `stream/landed/topic=…/partition=P/<first>-<last>.parquet`.
2. **Commit after write.** Only after the PUT succeeds does the lander commit `marker + 1` for that
   partition and upsert the `(hour, partition)` row in `ghflow_ops.stream_hours`.
   - A kill anywhere restarts from the last commit and reaches the same marker.
   - It rewrites the same object with the same bytes.
   - So the landed set covers every consumed offset exactly once, and `lander coverage` proves it
     from the table and the group's commits.
3. **A buffer never mixes hours.** If a record of another hour arrived before the marker, the
   lander refuses (crashes loudly) rather than file it under the wrong hour.
4. **An hour closes** when it has a committed file on all 6 partitions.
5. **Compaction runs only on closed hours** (DAG `stream_compact`, every 15 minutes).
   - It dedups by event id (keeping the first publication) and drops the markers.
   - It refuses a duplicate whose values disagree, and checks the output is content-equal to its
     inputs.
   - It writes `stream/compact/date=D/hour=HH/events.parquet` in bronze's schema.
   - It deletes the small files only after the compacted file and its `stream_compacted` row exist.
6. **The lander shuts down cleanly.** On SIGTERM or SIGINT it closes its consumer, so the group
   learns at once that the member left (gotcha #51).
7. **Lag is visible** in Console (group `lander`) and in `.\make.ps1 watch`.

## Options considered

- **Count- or time-based batches.** Rejected: a restart cuts different ranges, which overlap
  files and break exactly-once coverage. Marker boundaries are deterministic.
- **Kafka transactions or exactly-once sinks.** Rejected as unnecessary: deterministic file names
  plus commit-after-write give an idempotent sink, and duplicates from the replayer are removed by
  id at compaction.
- **Writing straight into DuckDB.** Rejected: DuckDB has one writer, `dbt_build` (ADR-0003). The
  lander writes Parquet to S3.

## Consequences

- Memory per partition is one hour's records (about 30k), fine at this scale.
- A replayer stuck mid-hour stalls that hour's landing; it is visible as lag, not as a gap.
- Backup: landed and compacted files regenerate from the topic, and the topic from bronze. The
  state is `stream_hours`, `stream_compacted` and the group's commits.

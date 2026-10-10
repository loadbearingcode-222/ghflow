# 0007: Batch and stream are reconciled hour by hour, by event id
Date: 2026-10-10 · Story: M2-S3 · Status: accepted

## Context

The stream starts from bronze, so the transport (replayer, topic, lander, compaction) is the only
place a difference can enter (plan: "one parse, two transports"). The project's central claim is
that the two paths agree to the event, and that claim needs a check that runs continuously and
names what disagrees.

## Decision

1. **Unit of comparison: one hour's set of event ids.** Batch: the ids in bronze. Stream: the
   ids in the hour's compacted file.
2. **Verdicts:**
   - **green:** the sets are equal;
   - **red:** some id is missing or extra. Each is named (up to `RECONCILE_NAMED_IDS`), and the
     counts are always exact;
   - **pending:** the stream has not closed the hour yet. That is not a failure.
3. **Duplicates are reported, never failed.** At-least-once delivery produces them by design
   (ADR-0005). Compaction removes them and records how many, and reconcile shows that count per
   hour.
4. **Results go to `ghflow_ops.reconcile_results`,** one row per hour per run. The DAG
   `reconcile` (hourly, at :45) checks the whole window and fails loudly on any red hour.
5. **The red team plants on a disposable copy:** one compacted hour with one event dropped,
   reconciled through a path override and deleted afterwards. The real stream files are never
   touched.
6. **A third witness joins the same comparison in M4-S1:** ClickHouse, the third id set.

## Consequences

- Each run reads two id columns per hour (bronze and compacted), eight hours in parallel. That
  is cheap enough to re-check the whole window every hour.
- A red hour's remedy is documented in the RUNBOOK: re-publish, and the duplicates are absorbed.

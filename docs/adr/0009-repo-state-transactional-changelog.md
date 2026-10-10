# 0009: Repo state is a compacted changelog written in Kafka transactions
Date: 2026-10-10 · Story: M3-S2 · Status: accepted

## Context

`dim_repo` (ADR-0008) gives each repo's current name once a day, from the batch path. The stream
should hold the same fact continuously, as a topic any consumer (ClickHouse in M4-S1) can read.
The processor that keeps it has state, so a crash must not lose changes, emit them twice, or
leave the state out of step with the consumed offsets.

## Decision

1. **`gharchive.repo_state`: 6 partitions, `cleanup.policy=compact`, keyed by repo id.** The
   same murmur2 key as the events, so a repo's state lives in the partition of its events.
   Compaction keeps the latest record per repo forever, while `gharchive.events` expires after 7
   days.
2. **Unit of work: one (partition, hour), closed by its END_OF_HOUR marker** (as the lander,
   ADR-0006). Within the hour, a repo's name is the one on its latest event by (created_at,
   event id), the same rule as `dim_repo`. The hour's internal order and any replayed duplicates
   therefore change nothing.
3. **Only net changes are written:** the repos whose hour-final name differs from the stored one
   and is newer than it. A → B → A within an hour writes nothing (the history is `dim_repo`'s
   job), and an old hour replayed by hand cannot move a repo back.
4. **One transaction per closed (partition, hour):** the change records plus the consumed offset
   past the marker (`send_offsets_to_transaction`). Both are committed, or neither. The consumer
   reads `read_committed`. A restart with the same `transactional.id` fences the old instance
   and aborts its open transaction.
5. **The store is a cache, rebuilt from the changelog on every start.** It is SQLite in the
   container, with no journal. The live store and a restored one hold the same values, because a
   record is written exactly when the stored value changes. A restart therefore re-reads the
   interrupted hour and emits only what was never committed.
6. **Checked against the batch path by `diff`.**
   - It compares the topic's latest record per repo with `dim_repo`'s current rows.
   - Only the overlap of the two witnesses is compared: the warehouse up to its newest staged
     hour, and the topic up to the hour the processor has committed past on every partition. The
     diff refuses (`pending`) while the processor is behind.
   - A record newer than the warehouse is `ahead`: counted, never failed.
   - Red verdicts name the repos: `stale`, `missing_in_topic`, `extra_in_topic`.

## Consequences

- **The startup cost grows with the number of repos.** A restore reads every committed record:
  365k records took 7.7 s, so about 13.6M should take minutes. M3-S3 measures and predicts the
  full restore, and may add a snapshot.
- **Replaying from scratch needs `gharchive.events`, which keeps only 7 days.** The compacted
  topic is the durable copy of the state. Rebuilding it from zero after expiry means replaying
  bronze first (the replayer can).
- **`valid_from` is not carried.** The topic says what a repo is called now, as of the event that
  named it; when it was called something else is answered by `dim_repo`.

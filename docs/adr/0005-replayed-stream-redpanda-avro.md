# 0005: The replayed stream — Redpanda, an Avro record generated from bronze, checkpoint after flush
Date: 2026-10-10 · Story: M2-S1 · Status: accepted

## Context

The second path publishes bronze to a topic. Later consumers must agree with the batch path to
the event: the lander (M2-S2), repo state (M3-S2) and ClickHouse (M4-S1). The stream must survive
kills without loss, and consumers must know when an hour is complete.

## Decision

1. **Redpanda v26.2.3**, one node in dev-container mode with 1 GiB (`mem_limit` 1.5 GiB), using
   its built-in Schema Registry, plus **Console v3.12.0**. Both are pinned by tag and digest,
   re-verified on 2026-10-10. Ports: Kafka 127.0.0.1:39092, Schema Registry 38081, Console 38080.
2. **Topics as code.** `config/redpanda/topics.conf` is applied on every `up` by `redpanda-init`:
   it creates topics, grows partitions and sets configs, and it **refuses to shrink**.
   `gharchive.events` has 6 partitions keyed by repo id and 7 days' retention, using
   **LogAppendTime**: event time is weeks old by replay and would expire at once. Partitions and
   retention mirror `settings` (tested).
3. **One record shape, generated from bronze** (`ghflow.stream.schema`). It holds every bronze
   column, nullable, plus `kind` (EVENT | END_OF_HOUR), `hour`, and the marker's `partition` and
   `rows_in_hour`. `config/schemas/gh_event.avsc` is its generated copy, drift-tested like silver.
4. **Named rules before Avro.** Avro cannot say "required when kind = EVENT", so the serializer
   refuses a record by rule (`EVENT_ID_REQUIRED`, `EVENT_TYPE_REQUIRED`,
   `EVENT_CREATED_AT_REQUIRED`, `EVENT_REPO_ID_REQUIRED`, `MARKER_PARTITION_REQUIRED`, …). It
   writes the Confluent wire format.
5. **BACKWARD compatibility, checked twice.** The registry subject is set to BACKWARD and refuses
   an incompatible schema. Before anything reaches a registry, CI's `avro-compat` job checks the
   generated schema against the one on main with an offline checker (`backward_problems`).
6. **End-of-hour markers travel as data.** After an hour's rows, one marker goes to EVERY
   partition, so each consumer can tell an hour is complete on the partitions it reads.
7. **Checkpoint after flush.** `ghflow_ops.replay_checkpoint` gets the hour's row only when
   delivery reports confirm rows plus markers with no failure (idempotent producer, acks=all).
   A kill mid-hour leaves no checkpoint and no markers. The restart republishes the whole hour,
   so consumers see duplicates (by event id), never a gap.
8. **One app image** (`docker/app`, python 3.12.13-slim, pinned) serves the stream services. The
   replayer service sits behind compose profile `replay` until the lander exists (M2-S2), so `up`
   never fills a topic that nothing reads.

## Options considered

- **Apache Kafka with the Confluent Schema Registry.** Rejected: two more services and ~2 GB
  more RAM, for no behaviour this project needs. Redpanda speaks the same APIs.
- **JSON records.** Rejected: there would be no registry-enforced compatibility, and the shape
  would be checked only by convention.
- **A checkpoint per message, or transactions.** Rejected here: transactions arrive where they
  are needed (the repo-state processor, M3-S2). Here the contract is at-least-once with counted
  duplicates, and the reconciliation (M2-S3) proves nothing is lost.

## Consequences

- Duplicates are expected after a crash, so every consumer dedups by event id (M2-S2, M4-S1).
- Changing the event shape means: change bronze/fields, regenerate the `.avsc`, and let CI check
  BACKWARD.
- Backup: the topic regenerates from bronze, because the replayer re-publishes. The only state is
  the checkpoint table, and it can be rebuilt by replaying.
- Versions: confluent-kafka 2.16.0 and fastavro 1.13.1 (planning observed 2.15.1 and 1.12.2).

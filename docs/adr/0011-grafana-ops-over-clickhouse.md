# 0011: Grafana reads everything through ClickHouse, as code, behind a gate
Date: 2026-10-10 · Story: M4-S2 · Status: accepted

## Context

Engineers need one place that shows whether the pipeline is healthy:
- ingest freshness;
- every consumer group's lag;
- reconciliation status;
- flagged hours.

That information lives in three systems: Postgres (`ghflow_ops`), Redpanda (committed offsets)
and ClickHouse.

## Decision

1. **One data source: ClickHouse.**
   - `ghflow_ops` is read live through PostgreSQL-engine tables (`ops_ingest_audit`,
     `ops_reconcile_results`, `ops_quality_verdicts`). Their credentials come from the named
     collection `ghflow_ops`, with the password from the environment, so nothing secret is
     committed.
   - Consumer lag is sampled every `LAG_SAMPLE_S` (15 s) by the `lag-sampler` service into
     `consumer_lag` (per group, topic and partition, TTL 30 days). Redpanda keeps no history,
     and a sampler costs 128 MiB, where Prometheus would cost far more.
2. **Dashboards are code.** `ghflow.commands.grafana` declares the panels.
   - The JSON under `docker/grafana/dashboards` is generated from it, and a unit test refuses
     drift.
   - Provisioning loads it, with UI edits off.
   - Every consumer group has its own lag panel, so one stopped consumer stands out against
     flat neighbours.
3. **The gate runs every panel's query through Grafana's own `/api/ds/query`.** That exercises
   provisioning, the plugin and ClickHouse together. It fails if any panel returns no rows,
   naming the panel. Red team: `gate --plant` adds one empty panel, which must be named alone.
4. **Access:** bound to 127.0.0.1. Anonymous users are Viewers, so the dashboard opens without
   a login; the admin password is in `.env`.

## Consequences

- A panel's latency is a ClickHouse query's latency: M4-S3 measures it, and tunes the tables
  behind it.
- On a transactional topic, a caught-up group shows a lag of 1 per partition: the trailing
  commit marker no consumer commits past. The line is flat, which is what the panel needs to
  show.

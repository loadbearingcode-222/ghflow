# Architecture

The blueprint of ghflow. GitHub renders the diagram below; the drawn version is
[img/architecture.svg](img/architecture.svg). This file is refreshed at every milestone close,
and from M5-S3 a checker fails if any compose service or DAG is missing from it.

![ghflow architecture blueprint](img/architecture.svg)

## The system

```mermaid
flowchart TB
  GA["GH Archive · hourly .json.gz · ~0.5 GB/day"]
  subgraph BATCH["Batch path · M1 · the Airflow data interval is the watermark"]
    ING["gharchive_ingest<br/>sensor · download · one cast · audit"]
    RAW[("MinIO raw<br/>kept 14 days")]
    BRZ[("MinIO bronze<br/>projected Parquet + sidecar")]
    DBT["dbt on DuckDB<br/>silver → gold SCD2 → marts"]
  end
  subgraph STREAM["Stream path · M2–M4 · committed offsets are the watermark"]
    REP["replayer<br/>checkpoint after broker ack"]
    RP[["Redpanda gharchive.events<br/>6 partitions · Avro · Schema Registry"]]
    LAN["lander<br/>files by offset range"]
    RS["repo-state processor<br/>transactions · compacted topic"]
    CH[("ClickHouse<br/>Kafka engine · ReplacingMergeTree · views")]
  end
  subgraph TRUST["Trust"]
    DQ["gharchive_quality<br/>volume + event mix vs baseline"]
    REC["reconcile<br/>batch = stream = ClickHouse"]
  end
  OPS[("Postgres ghflow_ops<br/>audit · checkpoints · verdicts")]
  GRA["Grafana · engineers · M4"]
  QUA["Quarto dashboard · stakeholders · M5<br/>localhost:33001 + standalone HTML"]
  GA --> ING
  ING --> RAW & BRZ
  BRZ --> DBT
  BRZ --> REP --> RP
  RP --> LAN & RS & CH
  ING -.-> OPS
  DBT --> DQ
  DQ -.-> OPS
  BRZ & LAN & CH --> REC
  RS -.->|agrees with dim_repo| DBT
  CH --> GRA
  DBT -->|mart snapshots| QUA
  DQ -->|flagged hours| QUA
```

Two boundaries the diagram encodes on purpose:
- **Airflow schedules the batch work; long-running services carry the stream.** Airflow runs the
  DAG-named boxes, plus `raw_retention`, `dbt_build`, `stream_compact` and `dashboard_render`. The
  replayer, lander and repo-state processor are services, never tasks that loop forever inside
  Airflow.
- **The stream starts from bronze, not from raw.** Parsing happens once, so any difference the
  reconciliation finds can only come from the stream transport.

## How each part knows what's new

| Part | Mechanism | Story |
|---|---|---|
| Batch ingest | The Airflow data interval is the hour; no stored watermark | M1-S1 |
| Upstream re-publication | ETag compared with the audit row | M1-S4 |
| Replayer | Its own checkpoint, moved only after the broker acks | M2-S1 |
| Lander | Committed consumer offsets, after the file is written | M2-S2 |
| Hour completeness | End-of-hour markers passed on every partition | M2-S2 |
| Repo state | Transactional offsets plus a compacted changelog | M3-S2 |
| ClickHouse | Kafka-engine offsets; materialized views see only new inserts | M4-S1 |
| Updates | SCD2 in dbt; ReplacingMergeTree keeps the latest version | M3-S1, M4-S1 |
| Stakeholder dashboard | Rebuilt after each daily build; refuses marts older than that build | M5-S2 |

## Where each piece lands

| Milestone | What becomes real | Release |
|---|---|---|
| M0 | Repo, plan, CI guards; source profile; event contract; MinIO, Postgres, Airflow | v0.0.0 |
| M1 | Hourly ingest, backfill from Aug 3, silver, event-mix quality checks | v0.1.0 |
| M2 | Redpanda + Schema Registry, replayer, lander, batch-vs-stream reconciliation | v0.2.0 |
| M3 | SCD2 repo/actor history; transactional compacted repo-state topic | v0.3.0 |
| M4 | ClickHouse consuming the topic; Grafana for engineers | v0.4.0 |
| M5 | Insight marts, the Quarto stakeholder dashboard, one-command demo | v1.0.0 |

Status at this revision (M0-S1): nothing runs yet; every box above is planned.

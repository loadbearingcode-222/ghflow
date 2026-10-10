# Architecture

The blueprint of ghflow. GitHub renders the diagram below; the drawn version is
[img/architecture.svg](img/architecture.svg). Both are refreshed in every Refine story, and from
M5-S4 a checker fails if any compose service or DAG is missing from them.

![ghflow architecture blueprint](img/architecture.svg)

## The system

```mermaid
flowchart TB
  GA["GH Archive · hourly .json.gz · ~0.5 GB/day"]
  subgraph BATCH["Batch path · M1 · the Airflow data interval is the watermark"]
    ING["gharchive_ingest<br/>sensor · download · one cast · audit"]
    RAW[("SeaweedFS raw<br/>kept 14 days")]
    BRZ[("SeaweedFS bronze<br/>projected Parquet + sidecar")]
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

| Milestone | Build stories make real | Refine story improves | Release |
|---|---|---|---|
| M0 | Repo, plan and CI guards; source profile; event contract; SeaweedFS, Postgres, Airflow | one event reader, the readability/cohesion ratchet, the first benchmark | v0.0.0 |
| M1 | Hourly ingest, backfill from Aug 3, silver, event-mix quality checks | ingest throughput, thin DAGs — same bronze bytes | v0.1.0 |
| M2 | Redpanda + Schema Registry, replayer, lander, batch-vs-stream reconciliation | stream throughput, one client configuration | v0.2.0 |
| M3 | SCD2 repo/actor history; transactional compacted repo-state topic | cheaper history, faster restore | v0.3.0 |
| M4 | ClickHouse consuming the topic; Grafana for engineers | table design, panel latency | v0.4.0 |
| M5 | Insight marts, the Quarto stakeholder dashboard, one-command demo | the whole system, end to end | v1.0.0 |

A later insight that changes an earlier design is handled by a Realign story inserted into the
plan (see `MILESTONES.md`), and this blueprint changes with it.

## Inside `src/ghflow`: a pure core and an I/O shell

Since M0-S5 the Python package has two layers. The arrows are the only imports allowed;
import-linter (CI job `import-layers`) and `tests/unit/test_laws.py` refuse any other.

```mermaid
flowchart TB
  subgraph SHELL["I/O shell · network, files, exit codes"]
    CMD["commands/<br/>profile · contract CLIs, each through guarded()"]
    ARC["archive<br/>HEAD/GET · SweepPolicy · download"]
  end
  subgraph CORE["pure core · no network, no files, no printing"]
    CON["contract<br/>the one cast · row rules · refusals"]
    PRO["profiling<br/>counts · era classifiers · bisection · the doc block"]
    EVT["events<br/>the one JSON-line reader"]
    FLD["fields<br/>keys · types · payload fields and their eras"]
    HRS["hours<br/>Hour values"]
    SET["settings<br/>every knob"]
  end
  EXC["exitcodes<br/>0–4 vocabulary"]
  CMD --> ARC & CON & PRO & EXC
  CON --> EVT & FLD
  PRO --> EVT
  ARC & CON & PRO --> HRS
  HRS --> SET
  ARC & CON & PRO --> SET
```

The DAGs (M1 onward) belong to the shell: they call core functions and hold no parsing logic.

## The batch path as it runs (M1)

```mermaid
flowchart LR
  GA["GH Archive<br/>hourly .json.gz"] -->|"sensor: published?"| ING["gharchive_ingest<br/>hourly · catchup on"]
  ING --> RAW[("raw .gz<br/>kept 14 days")]
  ING --> BRZ[("bronze Parquet<br/>+ rejected sidecar")]
  ING -.->|one row per attempt| AUD[("ingest_audit")]
  RET["raw_retention<br/>daily 01:00"] -.->|older than 14 days| RAW
  BRZ --> DBT["dbt_build · daily 02:30<br/>pool duckdb (one writer)"]
  DBT --> WH[("DuckDB warehouse<br/>staging table + 17 silver views<br/>+ gold dim_repo, dim_actor")]
  WH --> QUA["gharchive_quality · daily 03:30<br/>rolling clean baseline"]
  QUA -.-> VER[("quality_verdicts<br/>clean · flagged · shifted")]
  QUA -->|ETag changed| REQ[("reingest_requests")] --> ING
```

Every DAG is thin by law (`tests/unit/test_thin_dags.py`): it calls `python -m ghflow.commands.…`
in the project venv and maps exit codes; parsing, Parquet and SQL live in `src/ghflow`.

## The replayed stream as it runs (M2)

```mermaid
flowchart LR
  BRZ[("bronze Parquet")] --> REP["replayer · service<br/>checkpoint after flush"]
  REP -->|Avro, keyed by repo id<br/>+ END_OF_HOUR per partition| RP[["gharchive.events<br/>6 partitions · LogAppendTime · BACKWARD"]]
  RP --> LAN["lander · service<br/>one file per (partition, hour)<br/>commit after write"]
  LAN --> LND[("stream/landed")]
  LND --> CMP["stream_compact · 15 min<br/>closed hours · dedup by id"]
  CMP --> SC[("stream/compact")]
  BRZ & SC --> REC["reconcile · hourly<br/>batch ids = stream ids"]
  REC -.-> RR[("reconcile_results")]
  REP -.-> CK[("replay_checkpoint")]
  LAN -.-> SH[("stream_hours")]
```

Every Kafka client is built by `ghflow.stream.clients` (a test refuses client settings anywhere
else).

## History and repo state as they run (M3)

```mermaid
flowchart LR
  STG[("staging<br/>every event")] --> GOLD["dbt gold · each dbt_build<br/>full recompute (ADR-0008)"]
  GOLD --> DIM[("dim_repo · dim_actor<br/>SCD2: islands of one name")]
  RP[["gharchive.events"]] --> RS["repo-state · service<br/>one transaction per (partition, hour)<br/>changes + offsets"]
  RS --> RST[["gharchive.repo_state<br/>compacted · keyed by repo id"]]
  RS <-.->|"store + position<br/>(volume repo-state)"| SQL[("SQLite")]
  RST -.->|"start: tail after the stored position"| RS
  RST & DIM --> DIFF["repo_state diff<br/>latest per repo = dim_repo current"]
```

Two witnesses of the same fact, built differently:
- **Batch:** `dim_repo` is a function of every staged event, recomputed in full, so arrival order
  cannot matter (proved by `history late-hour`).
- **Stream:** `gharchive.repo_state` is a log of net changes committed atomically with the
  offsets. Its store survives restarts with the changelog position it reflects; a start reads
  only the tail, and a missing or impossible store means a full rebuild.
- **`diff`** compares the two over the hours both have seen. It found 0 stale, missing or extra
  repos over 13.59M repos.

## ClickHouse and Grafana as they run (M4)

```mermaid
flowchart LR
  RP[["gharchive.events"]] -->|Kafka engine · group clickhouse| EQ["events_queue"]
  RST[["gharchive.repo_state"]] -->|Kafka engine · group clickhouse-repo-state| SQ["repo_state_queue"]
  EQ -->|"kind = EVENT"| EV[("events<br/>ReplacingMergeTree (hour, id)")]
  EQ -->|"kind = END_OF_HOUR"| HM[("hour_markers")]
  EQ --> EH[("events_hourly<br/>groupBitmap per (hour, type)")]
  EH -->|"refresh every 5 min"| EC[("events_hour_counts")]
  SQ --> RC[("repo_current<br/>ReplacingMergeTree(as_of)")]
  PG[("Postgres ghflow_ops")] -.->|PostgreSQL engine| OPS[("ops_ingest_audit · ops_reconcile_results<br/>ops_quality_verdicts")]
  LS["lag-sampler · 15 s"] --> CL[("consumer_lag")]
  EC & OPS & CL --> GF["Grafana · ghflow-ops<br/>gate: every panel returns rows"]
  EV & HM --> REC["reconcile --clickhouse<br/>the third witness"]
```

- **Every table tolerates a re-read block.** The Kafka engine delivers at least once, and ReplacingMergeTree, bitmap states and markers-as-data absorb the duplicates (ADR-0010).
- **Grafana reads only ClickHouse** (ADR-0011). The ops data comes through PostgreSQL-engine tables, and lag through the sampler.

Status at this revision (M4-S3, `v0.4.0`):
- **Batch path:** runs end to end over the whole window (Aug 3 onward, hourly since), with the SCD2 dimensions.
- **Replayed stream:** carries the same window. Batch = stream = ClickHouse for every hour (three witnesses), and the repo-state topic equals `dim_repo`.
- **Ops:** the Grafana dashboard shows freshness, lag per consumer group, reconciliation and flagged hours.
- **Still planned:** the stakeholder dashboard (M5).

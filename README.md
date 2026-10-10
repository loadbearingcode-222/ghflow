# ghflow

How does a pipeline know what is new when a dataset keeps changing? ghflow answers that on
[GH Archive](https://www.gharchive.org/) — every public GitHub event, one file per hour — through
a batch path and a replayed stream that must agree to the event, then tells non-technical readers
what the data shows.

The plan, the repo layout and every story: [docs/MILESTONES.md](docs/MILESTONES.md).
This README grows as each milestone closes.

## Where it stands (`v0.4.0`, M4)

- **Batch path:** hourly ingest from 2026-08-03 into bronze, then silver on DuckDB with
  drift-aware contracts and event-mix quality checks. Repo and actor name history is kept as
  SCD2 dimensions (`dim_repo`, `dim_actor`).
- **Replayed stream:** Redpanda and the Schema Registry, a lander that commits after it writes,
  and a repo-state processor that writes a compacted topic in Kafka transactions.
- **Agreement:**
  - batch, stream and ClickHouse reconcile hour by hour to the event id;
  - the repo-state topic equals `dim_repo`'s current rows.
- **ClickHouse** consumes both topics itself and is the third witness. **Grafana**
  (http://127.0.0.1:33000) shows ingest freshness, consumer lag per group, reconciliation and
  flagged hours.
- **Next:** the stakeholder dashboard (M5).

How it is built: [docs/architecture.md](docs/architecture.md) · decisions: [docs/adr/](docs/adr/)
· traps: [docs/gotchas.md](docs/gotchas.md).

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

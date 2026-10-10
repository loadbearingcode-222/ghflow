# KPI register

Every number on the stakeholder dashboard comes from one of these KPIs. Each KPI has exactly one
dbt mart in `dbt/ghflow/models/marts` (linked by `meta.kpi`), and a unit test fails on a mart
without an entry or an entry without a mart. A changed formula gets a new id; the old row stays,
marked retired.

**Broken hours are defined once:** a KPI reads each hour's trust from
`stg_ops__hour_verdicts`, the latest verdict `gharchive_quality` wrote (ADR-0004).
- **`flagged` hours** (volume or PushEvent broke) are counted and shown, and are left out of every
  average, share and ranking.
- **`shifted` hours** (only minor event types moved) are kept.

**Window:** from 2026-08-03 (UTC). Daily KPIs use complete days only: all 24 hours ingested, before
today.

**Owner:** role `data analyst`. **Twin:** each headline is recomputed by
`python -m ghflow.commands.marts twin`, using independent SQL over silver plus a direct read of
`ghflow_ops`, never the marts.

| Id | KPI | Mart | Formula (headline) | Source | Window |
|---|---|---|---|---|---|
| K01 | Typical day | `mart_daily_activity` | median over complete days of (events in non-flagged hours ÷ non-flagged hours × 24) | staging | complete days |
| K02 | Push share | `mart_event_mix` | PushEvents ÷ all events, non-flagged hours of complete days | staging | complete days |
| K03 | Busiest hour of the week | `mart_hour_of_week` | the (weekday, UTC hour) with the highest average events per non-flagged hour | staging | whole window |
| K04 | Rising repo | `mart_rising_repos` | stars (WatchEvents) in the newest 14 complete days, non-flagged hours, top repo | staging + dim_repo | 28 days |
| K05 | Bot share | `mart_bot_share` | events by logins ending in `[bot]` ÷ all events, non-flagged hours of complete days | staging | complete days |
| K06 | Busiest bot | `mart_top_bots` | events by the top `[bot]` login, non-flagged hours | staging | whole window |
| K07 | Reconciled hours | `mart_trust` | hours whose latest three-witness check is green ÷ hours ingested | ghflow_ops.reconcile_results | whole window |

-- Repository names over time (SCD2), from the event log (M3-S1, ADR-0008).
-- Incremental (M3-S3): only the ids with events in staging's reload window are recomputed,
-- each over its whole history; `--full-refresh` recomputes every id.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='repo_id',
    on_schema_change='fail',
) }}

select
    key_id as repo_id,
    name as repo_name,
    valid_from,
    valid_to,
    valid_to is null as is_current
from ({{ scd2_from_events('repo_id', 'repo_name') }})

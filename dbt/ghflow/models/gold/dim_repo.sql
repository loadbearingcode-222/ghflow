-- Repository names over time (SCD2), from the event log (M3-S1, ADR-0008).
{{ config(materialized='table') }}

select
    key_id as repo_id,
    name as repo_name,
    valid_from,
    valid_to,
    valid_to is null as is_current
from ({{ scd2_from_events('repo_id', 'repo_name') }})

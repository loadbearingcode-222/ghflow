-- Actor logins over time (SCD2), from the event log (M3-S1, ADR-0008).
{{ config(materialized='table') }}

select
    key_id as actor_id,
    name as actor_login,
    valid_from,
    valid_to,
    valid_to is null as is_current
from ({{ scd2_from_events('actor_id', 'actor_login') }})

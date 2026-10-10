-- K07 Can we trust it? (docs/KPIS.md): per day, hours ingested, the quality verdicts (the one
-- definition of broken: stg_ops__hour_verdicts) and how many hours the three witnesses
-- reconciled green in their latest check (ADR-0007, ADR-0010).
with reconciled as (
    select
        cast(left(hour, 10) as date) as event_date,
        cast(split_part(hour, '-', 4) as integer) as event_hour,
        arg_max(status, checked_at) as status
    from ops.public.reconcile_results
    group by hour
),

hours as (
    select event_date, event_hour, any_value(trust) as trust
    from {{ ref('int_hourly_events') }}
    group by all
)

select
    h.event_date,
    count(*) as hours_ingested,
    count(*) filter (where h.trust = 'flagged') as flagged_hours,
    count(*) filter (where h.trust = 'shifted') as shifted_hours,
    count(*) filter (where h.trust in ('clean', 'unjudged')) as clean_hours,
    count(*) filter (where r.status = 'green') as reconciled_green,
    count(*) filter (where r.status = 'red') as reconciled_red
from hours h
left join reconciled r on r.event_date = h.event_date and r.event_hour = h.event_hour
group by h.event_date
order by h.event_date

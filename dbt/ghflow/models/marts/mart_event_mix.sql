-- K02 Event mix by day (docs/KPIS.md): each type's share of the day's events, flagged hours out.
with clean as (
    select h.event_date, h.type, sum(h.events) as events
    from {{ ref('int_hourly_events') }} h
    semi join {{ ref('int_complete_days') }} d on d.event_date = h.event_date
    where h.trust <> 'flagged'
    group by all
)

select
    event_date,
    type,
    events,
    events / sum(events) over (partition by event_date) as share
from clean
order by event_date, events desc

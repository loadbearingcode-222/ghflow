-- K03 When the world codes (docs/KPIS.md): average events per hour by weekday and hour of day
-- (UTC), over clean hours only.
with hours as (
    select event_date, event_hour, sum(events) as events
    from {{ ref('int_hourly_events') }}
    where trust <> 'flagged'
    group by all
)

select
    isodow(event_date) as weekday,
    dayname(event_date) as weekday_name,
    event_hour as hour_utc,
    count(*) as hours_observed,
    round(avg(events)) as avg_events
from hours
group by all
order by weekday, hour_utc

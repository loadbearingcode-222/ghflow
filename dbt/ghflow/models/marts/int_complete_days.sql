-- Days with all 24 hours ingested, before today (M5-S1): daily KPIs use only these.
select event_date
from {{ ref('int_hourly_events') }}
where event_date < current_date
group by event_date
having count(distinct event_hour) = 24

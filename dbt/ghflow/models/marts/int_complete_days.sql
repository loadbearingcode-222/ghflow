-- Days with all 24 hours ingested AND judged, before today (M5-S1; judged since M5-S3): daily
-- KPIs use only these, so a day never reaches the dashboard before its broken hours are known.
select event_date
from {{ ref('int_hourly_events') }}
where event_date < current_date
group by event_date
having count(distinct event_hour) = 24
   and count(distinct event_hour) filter (where trust <> 'unjudged') = 24

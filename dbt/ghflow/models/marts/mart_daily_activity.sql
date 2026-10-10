-- K01 Daily activity (docs/KPIS.md). Flagged hours are counted, never averaged: the day's rate
-- uses its other hours, scaled to 24.
with hours as (
    select h.event_date, h.event_hour, h.trust, sum(h.events) as events
    from {{ ref('int_hourly_events') }} h
    semi join {{ ref('int_complete_days') }} d on d.event_date = h.event_date
    group by all
),

flagged as (
    select event_date, event_hour from {{ ref('stg_ops__hour_verdicts') }} where status = 'flagged'
),

clean_events as (
    select s.event_date, s.actor_id, s.repo_id
    from {{ ref('stg_gharchive__events') }} s
    semi join {{ ref('int_complete_days') }} d on d.event_date = s.event_date
    anti join flagged f on f.event_date = s.event_date and f.event_hour = s.event_hour
),

-- Distinct (day, id) pairs as grouping keys first, then counted: a per-day distinct set over
-- 147M events would hold every id at once (gotcha #52).
actor_days as (
    select event_date, count(*) as actors
    from (select event_date, actor_id from clean_events group by all)
    group by event_date
),

repo_days as (
    select event_date, count(*) as repos
    from (select event_date, repo_id from clean_events group by all)
    group by event_date
),

people as (
    select a.event_date, a.actors, r.repos from actor_days a join repo_days r using (event_date)
)

select
    h.event_date,
    count(*) as hours_ingested,
    count(*) filter (where h.trust = 'flagged') as flagged_hours,
    count(*) filter (where h.trust = 'shifted') as shifted_hours,
    sum(h.events) as events,
    round(24.0 * sum(h.events) filter (where h.trust <> 'flagged')
          / nullif(count(*) filter (where h.trust <> 'flagged'), 0)) as events_per_day_clean,
    any_value(p.actors) as actors_clean_hours,
    any_value(p.repos) as repos_clean_hours
from hours h
left join people p on p.event_date = h.event_date
group by h.event_date
order by h.event_date

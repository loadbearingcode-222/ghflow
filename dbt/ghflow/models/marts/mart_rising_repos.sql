-- K04 What's rising (docs/KPIS.md): stars (WatchEvents) in the newest 14 complete days against
-- the 14 before, clean hours only; names from dim_repo's current row (ADR-0008). Repos, never
-- people, are ranked. Counted by repo id first, named after (gotcha #52).
with bounds as (
    select max(event_date) as last_day from {{ ref('int_complete_days') }}
),

flagged as (
    select event_date, event_hour from {{ ref('stg_ops__hour_verdicts') }} where status = 'flagged'
),

stars as (
    select
        s.repo_id,
        count(*) filter (where s.event_date > b.last_day - 14) as stars_recent,
        count(*) filter (where s.event_date <= b.last_day - 14) as stars_prior
    from {{ ref('stg_gharchive__events') }} s
    cross join bounds b
    anti join flagged f on f.event_date = s.event_date and f.event_hour = s.event_hour
    where s.type = 'WatchEvent' and s.event_date between b.last_day - 27 and b.last_day
    group by s.repo_id
),

top as (
    select * from stars order by stars_recent desc, repo_id limit 25
)

select
    row_number() over (order by t.stars_recent desc, t.repo_id) as rank,
    t.repo_id,
    r.repo_name,
    t.stars_recent,
    t.stars_prior,
    t.stars_recent - t.stars_prior as growth,
    (select last_day from bounds) as window_end
from top t
left join {{ ref('dim_repo') }} r on r.repo_id = t.repo_id and r.is_current
order by rank

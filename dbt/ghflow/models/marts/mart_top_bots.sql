-- K06 The busiest bots (docs/KPIS.md): events by GitHub App bot over the window, clean hours
-- only. Bots may be named; people never are (the dashboard's no-person test).
with flagged as (
    select event_date, event_hour from {{ ref('stg_ops__hour_verdicts') }} where status = 'flagged'
),

counts as (
    select s.actor_login as bot, count(*) as events
    from {{ ref('stg_gharchive__events') }} s
    anti join flagged f on f.event_date = s.event_date and f.event_hour = s.event_hour
    where s.actor_login like '%[bot]'
    group by s.actor_login
)

select row_number() over (order by events desc, bot) as rank, bot, events
from counts
order by rank
limit 20

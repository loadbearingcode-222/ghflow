-- Events per (hour, type) with each hour's trust status (M5-S1): the base most marts share, so
-- the event log is scanned once per build. Bots are GitHub App accounts: logins ending in [bot].
select
    s.event_date,
    s.event_hour,
    s.type,
    coalesce(v.status, 'unjudged') as trust,
    count(*) as events,
    count(*) filter (where s.actor_login like '%[bot]') as bot_events
from {{ ref('stg_gharchive__events') }} s
left join {{ ref('stg_ops__hour_verdicts') }} v
    on v.event_date = s.event_date and v.event_hour = s.event_hour
group by all

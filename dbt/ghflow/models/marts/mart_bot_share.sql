-- K05 Bots and people (docs/KPIS.md): the share of each day's events made by GitHub App bots
-- (logins ending in [bot]), clean hours only.
select
    h.event_date,
    sum(h.events) as events,
    sum(h.bot_events) as bot_events,
    sum(h.bot_events) / sum(h.events) as bot_share
from {{ ref('int_hourly_events') }} h
semi join {{ ref('int_complete_days') }} d on d.event_date = h.event_date
where h.trust <> 'flagged'
group by h.event_date
order by h.event_date

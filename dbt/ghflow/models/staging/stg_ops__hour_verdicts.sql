-- The ONE definition of a broken hour (M5-S1, ADR-0012): the latest verdict per hour that
-- gharchive_quality wrote to ghflow_ops.quality_verdicts (ADR-0004), read live and read-only.
-- `flagged` = volume or PushEvent broke (excluded from every average, share and ranking, and
-- shaded on the dashboard); `shifted` = only minor types moved (kept); `clean`. An hour with no
-- verdict yet is `unjudged` and treated like clean.
{{ config(materialized='table') }}

select
    cast(hour_start as timestamptz) as hour_start,
    cast(hour_start as date) as event_date,
    cast(extract(hour from hour_start) as integer) as event_hour,
    arg_max(status, judged_at) as status
from ops.public.quality_verdicts
group by hour_start

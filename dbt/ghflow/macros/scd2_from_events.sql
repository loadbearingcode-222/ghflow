{#
  SCD2 history of a name over the event log (M3-S1, ADR-0008). Deterministic: a full recompute
  from staging, so the order hours arrived in cannot change the result.

  - One pass groups every event by (id, name) with its first sighting. An id with one pair is one
    version (~99% of ids), done. The name is a grouping key, not an aggregate state: DuckDB
    spills keys, but string aggregate states over 13.6M groups ran a 3 GiB container out of
    memory (gotcha #52).
  - Only the events of multi-name ids go further: one name per (id, second), the name of the
    latest event (highest event id) in that second, so two versions never start in the same
    second (valid_from strictly increasing).
  - A version is an island of consecutive seconds with the same name: A -> B -> A is three
    versions, never merged.
  - valid_to is the next version's valid_from (no gap, no overlap); the last version is current.
  - Incremental runs (M3-S3) recompute only the ids with events in staging's reload window (the
    days staging just deleted and re-read), each over its WHOLE history, so the rows written equal
    a full recompute's rows for those ids; the model deletes and inserts by id.
#}
{% macro scd2_from_events(id_column, name_column) %}
with source as (
    select e.id, e.created_at, e.{{ id_column }} as key_id, e.{{ name_column }} as name
    from {{ ref('stg_gharchive__events') }} e
    {% if is_incremental() %}
    semi join (
        select distinct {{ id_column }} as key_id from {{ ref('stg_gharchive__events') }}
        where event_date >= (select max(event_date) - {{ var('lookback_days') }} from {{ ref('stg_gharchive__events') }})
    ) touched on e.{{ id_column }} = touched.key_id
    {% endif %}
    where e.{{ id_column }} is not null and e.{{ name_column }} is not null
),

pairs as (
    select key_id, name, min(created_at) as first_seen
    from source
    group by all
),

multi as (
    select key_id from pairs group by key_id having count(*) > 1
),

per_second as (
    select s.key_id, s.created_at, arg_max(s.name, s.id) as name
    from source s
    semi join multi m on s.key_id = m.key_id
    group by all
),

changes as (
    select
        key_id, name, created_at,
        case when name is distinct from lag(name) over (partition by key_id order by created_at)
             then 1 else 0 end as starts_version
    from per_second
),

islands as (
    select key_id, name, created_at,
           sum(starts_version) over (partition by key_id order by created_at rows unbounded preceding) as island
    from changes
),

versions as (
    select p.key_id, p.name, p.first_seen as valid_from
    from pairs p anti join multi m on p.key_id = m.key_id
    union all
    select key_id, any_value(name) as name, min(created_at) as valid_from
    from islands
    group by key_id, island
)

select
    key_id,
    name,
    valid_from,
    lead(valid_from) over (partition by key_id order by valid_from) as valid_to
from versions
{% endmacro %}

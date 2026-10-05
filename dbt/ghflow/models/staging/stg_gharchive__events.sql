-- Every bronze row, typed as bronze typed it, plus the hour it came from (Hive partitions).
-- Incremental by day: a run deletes and re-reads the newest `lookback_days` days of bronze, so
-- it is idempotent and picks up re-ingested recent hours; `--full-refresh` rebuilds the window.
-- One key column on purpose: dbt-duckdb's delete+insert on a composite key took >10 minutes
-- for a single day (M1-S3).
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
) }}

with bronze as (
    select
        * exclude (date, hour),
        cast(date as date) as event_date,
        cast(hour as integer) as event_hour
    from read_parquet('{{ var("bronze_events") }}', hive_partitioning = true, union_by_name = true)
)

select * from bronze
{% if is_incremental() %}
where event_date >= (select max(event_date) - {{ var('lookback_days') }} from {{ this }})
{% endif %}

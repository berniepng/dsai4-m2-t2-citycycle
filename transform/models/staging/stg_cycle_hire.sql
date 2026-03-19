-- models/staging/stg_cycle_hire.sql
-- ==================================
-- Cleans and types the raw cycle_hire table.
-- One row per rental. No business logic yet — that's intermediate's job.
--
-- Changes from raw:
--   • Rename duration → duration_seconds (explicit unit)
--   • Parse timestamps to TIMESTAMP type
--   • Add hire_date (DATE) for partitioning downstream
--   • Cast station IDs to INT64 (defensive — source occasionally ships strings)
--   • Filter out clear data errors (negative duration, null rental_id)

{{
  config(
    materialized = 'view',
    tags         = ['staging', 'cycle_hire']
  )
}}

with source as (

    select * from {{ source('raw', 'cycle_hire') }}
    -- Dev cost guard: limit rows when is_dev = true
    {% if var('is_dev', false) %}
    limit {{ var('dev_limit', 1000) }}
    {% endif %}

),

cleaned as (

    select
        -- ── identifiers ──────────────────────────────────────────
        cast(rental_id       as int64)    as rental_id,
        cast(bike_id         as int64)    as bike_id,

        -- ── station references ────────────────────────────────────
        cast(start_station_id as int64)   as start_station_id,
        start_station_name,
        cast(end_station_id   as int64)   as end_station_id,
        end_station_name,

        -- ── timestamps ────────────────────────────────────────────
        cast(start_date as timestamp)     as start_datetime,
        cast(end_date   as timestamp)     as end_datetime,

        -- ── derived date fields ───────────────────────────────────
        date(cast(start_date as timestamp))         as hire_date,
        extract(hour from cast(start_date as timestamp))  as start_hour,
        extract(dayofweek from cast(start_date as timestamp)) as day_of_week,
        -- BigQuery: 1=Sunday … 7=Saturday
        case
            when extract(dayofweek from cast(start_date as timestamp)) in (1, 7)
            then true else false
        end                                         as is_weekend,

        -- ── duration ──────────────────────────────────────────────
        cast(duration as int64)           as duration_seconds

    from source

    where
        -- remove rows with missing primary key
        rental_id is not null
        -- remove physically impossible durations
        and cast(duration as int64) between 60 and 86400
        -- remove rows where end is before start (data quality issue in source)
        and cast(end_date as timestamp) > cast(start_date as timestamp)

)

select * from cleaned

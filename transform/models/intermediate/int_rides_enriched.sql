-- models/intermediate/int_rides_enriched.sql
-- =============================================
-- Joins staging rides with station data.
-- Adds business logic flags needed for rebalancing analysis:
--   • peak_hour_flag     — AM/PM commuter peaks
--   • duration_band      — short/medium/long/extended
--   • duration_minutes   — human-readable duration
--
-- This is NOT the final fact table — marts/fact_rides.sql will
-- add the imbalance score and weekly demand index.

{{
  config(
    materialized = 'view',
    tags         = ['intermediate']
  )
}}

with rides as (

    select * from {{ ref('stg_cycle_hire') }}

),

stations as (

    select
        station_id,
        station_name,
        zone,
        nb_docks,
        capacity_tier,
        latitude,
        longitude
    from {{ ref('stg_cycle_stations') }}

),

enriched as (

    select
        -- ── ride identifiers ──────────────────────────────────────
        r.rental_id,
        r.bike_id,

        -- ── timing ───────────────────────────────────────────────
        r.start_datetime,
        r.end_datetime,
        r.hire_date,
        r.start_hour,
        r.day_of_week,
        r.is_weekend,

        -- ── duration ──────────────────────────────────────────────
        r.duration_seconds,
        round(r.duration_seconds / 60.0, 1)  as duration_minutes,

        -- Duration band — useful for customer segmentation
        case
            when r.duration_seconds < 600   then 'short'      -- < 10 min
            when r.duration_seconds < 1800  then 'medium'     -- 10-30 min
            when r.duration_seconds < 3600  then 'long'       -- 30-60 min
            else                                 'extended'   -- > 60 min
        end                                 as duration_band,

        -- ── peak hour flag ────────────────────────────────────────
        -- AM peak: 07:00–09:00, PM peak: 17:00–19:00 (London commuter pattern)
        case
            when r.start_hour in (7, 8, 17, 18) then 1
            else 0
        end                                 as peak_hour_flag,

        -- Finer-grained time period (useful for ML features)
        case
            when r.start_hour between 7  and 9  then 'am_peak'
            when r.start_hour between 17 and 19 then 'pm_peak'
            when r.start_hour between 10 and 16 then 'midday'
            when r.start_hour between 20 and 23 then 'evening'
            else                                     'night'
        end                                 as time_period,

        -- ── start station ─────────────────────────────────────────
        r.start_station_id,
        r.start_station_name,
        ss.zone                             as start_zone,
        ss.nb_docks                         as start_nb_docks,
        ss.capacity_tier                    as start_capacity_tier,
        ss.latitude                         as start_lat,
        ss.longitude                        as start_lon,

        -- ── end station ───────────────────────────────────────────
        r.end_station_id,
        r.end_station_name,
        es.zone                             as end_zone,
        es.nb_docks                         as end_nb_docks,
        es.capacity_tier                    as end_capacity_tier,
        es.latitude                         as end_lat,
        es.longitude                        as end_lon,

        -- ── same-station return flag ──────────────────────────────
        case
            when r.start_station_id = r.end_station_id then true
            else false
        end                                 as is_round_trip

    from rides r

    left join stations ss
        on r.start_station_id = ss.station_id

    left join stations es
        on r.end_station_id = es.station_id

)

select * from enriched

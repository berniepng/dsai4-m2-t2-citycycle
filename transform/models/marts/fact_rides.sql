-- models/marts/fact_rides.sql
-- =============================
-- Gold layer. Final fact table for all analytics and ML.
-- Partitioned by hire_date, clustered by start/end station_id.
--
-- One row per rental. All joins resolved. All business flags added.
-- This is what dashboards, Looker Studio, and the ML model query.

{{
  config(
    materialized   = 'table',
    partition_by   = {
      "field": "hire_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by     = ["start_station_id", "end_station_id"],
    tags           = ['marts', 'fact']
  )
}}

with rides as (

    select * from {{ ref('int_rides_enriched') }}

),

station_daily as (

    select
        hire_date,
        station_id,
        imbalance_score,
        is_imbalanced,
        imbalance_direction,
        net_flow,
        utilisation_rate,
        -- Rolling 7-day average demand (used as ML feature)
        avg(total_departures) over (
            partition by station_id
            order by hire_date
            rows between 6 preceding and current row
        ) as rolling_7d_avg_departures

    from {{ ref('int_station_daily_stats') }}

),

final as (

    select
        -- ── surrogate key ─────────────────────────────────────────
        {{ dbt_utils.generate_surrogate_key(['r.rental_id']) }}  as ride_sk,

        -- ── identifiers ──────────────────────────────────────────
        r.rental_id,
        r.bike_id,

        -- ── timing ───────────────────────────────────────────────
        r.hire_date,
        r.start_datetime,
        r.end_datetime,
        r.start_hour,
        r.day_of_week,
        r.is_weekend,
        r.time_period,
        r.peak_hour_flag,

        -- ── duration ──────────────────────────────────────────────
        r.duration_seconds,
        r.duration_minutes,
        r.duration_band,

        -- ── start station ─────────────────────────────────────────
        r.start_station_id,
        r.start_station_name,
        r.start_zone,
        r.start_nb_docks,
        r.start_capacity_tier,
        r.start_lat,
        r.start_lon,

        -- ── end station ───────────────────────────────────────────
        r.end_station_id,
        r.end_station_name,
        r.end_zone,
        r.end_nb_docks,
        r.end_capacity_tier,
        r.end_lat,
        r.end_lon,

        r.is_round_trip,

        -- ── rebalancing signals (from station daily stats) ────────
        coalesce(sd.imbalance_score,     0)     as start_station_imbalance_score,
        coalesce(sd.is_imbalanced,       false) as start_station_is_imbalanced,
        coalesce(sd.imbalance_direction, 'balanced') as start_station_imbalance_direction,
        coalesce(sd.net_flow,            0)     as start_station_net_flow,
        coalesce(sd.utilisation_rate,    0)     as start_station_utilisation_rate,
        coalesce(sd.rolling_7d_avg_departures, 0) as start_station_rolling_7d_avg

    from rides r

    left join station_daily sd
        on r.hire_date      = sd.hire_date
        and r.start_station_id = sd.station_id

)

select * from final

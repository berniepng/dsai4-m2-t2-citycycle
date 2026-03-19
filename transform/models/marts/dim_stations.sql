-- models/marts/dim_stations.sql
-- ================================
-- Station dimension table for the star schema.
-- Includes latest imbalance stats joined from int_station_daily_stats.
-- One row per station.

{{
  config(
    materialized = 'table',
    tags         = ['marts', 'dimension']
  )
}}

with stations as (

    select * from {{ ref('stg_cycle_stations') }}

),

-- Get the most recent daily stats per station (for "current" imbalance status)
latest_stats as (

    select
        station_id,
        avg(imbalance_score)      as avg_imbalance_score_7d,
        avg(utilisation_rate)     as avg_utilisation_rate_7d,
        sum(total_departures)     as total_departures_all_time,
        sum(total_arrivals)       as total_arrivals_all_time,
        max(hire_date)            as last_activity_date

    from {{ ref('int_station_daily_stats') }}

    -- Last 7 days of data for "recent" metrics
    where hire_date >= date_sub(
        (select max(hire_date) from {{ ref('int_station_daily_stats') }}),
        interval 7 day
    )

    group by 1

),

final as (

    select
        -- ── surrogate key ─────────────────────────────────────────
        {{ dbt_utils.generate_surrogate_key(['s.station_id']) }} as station_sk,

        -- ── identifiers ──────────────────────────────────────────
        s.station_id,
        s.station_name,
        s.terminal_name,

        -- ── location ──────────────────────────────────────────────
        s.latitude,
        s.longitude,
        s.zone,

        -- ── capacity ──────────────────────────────────────────────
        s.nb_docks,
        s.capacity_tier,

        -- ── operational ───────────────────────────────────────────
        s.is_installed,
        s.is_locked,
        s.is_temporary,
        s.install_date,

        -- ── rebalancing metrics (7-day rolling) ──────────────────
        coalesce(ls.avg_imbalance_score_7d,    0)  as avg_imbalance_score_7d,
        coalesce(ls.avg_utilisation_rate_7d,   0)  as avg_utilisation_rate_7d,
        coalesce(ls.total_departures_all_time, 0)  as total_departures_all_time,
        coalesce(ls.total_arrivals_all_time,   0)  as total_arrivals_all_time,
        ls.last_activity_date,

        -- ── rebalancing priority tier ─────────────────────────────
        case
            when coalesce(ls.avg_imbalance_score_7d, 0) >= 0.5 then 'CRITICAL'
            when coalesce(ls.avg_imbalance_score_7d, 0) >= 0.3 then 'HIGH'
            when coalesce(ls.avg_imbalance_score_7d, 0) >= 0.1 then 'MEDIUM'
            else                                                      'LOW'
        end as rebalancing_priority

    from stations s

    left join latest_stats ls
        on s.station_id = ls.station_id

)

select * from final

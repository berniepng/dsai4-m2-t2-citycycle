-- models/intermediate/int_station_daily_stats.sql
-- ==================================================
-- Aggregates rides to station-day level.
-- Computes net flow (departures - arrivals) per station per day.
-- This is the key signal for the rebalancing problem.
--
-- Net flow interpretation:
--   > 0  → more departures than arrivals (station drains — needs restocking)
--   < 0  → more arrivals than departures (station fills — needs emptying)
--   = 0  → balanced

{{
  config(
    materialized = 'view',
    tags         = ['intermediate', 'rebalancing']
  )
}}

with departures as (

    select
        hire_date,
        start_station_id       as station_id,
        start_station_name     as station_name,
        start_zone             as zone,
        start_nb_docks         as nb_docks,
        start_lat              as latitude,
        start_lon              as longitude,
        count(*)               as total_departures,
        countif(peak_hour_flag = 1)  as peak_departures,
        avg(duration_minutes)  as avg_duration_mins

    from {{ ref('int_rides_enriched') }}
    group by 1, 2, 3, 4, 5, 6, 7

),

arrivals as (

    select
        hire_date,
        end_station_id         as station_id,
        count(*)               as total_arrivals,
        countif(peak_hour_flag = 1)  as peak_arrivals

    from {{ ref('int_rides_enriched') }}
    group by 1, 2

),

joined as (

    select
        d.hire_date,
        d.station_id,
        d.station_name,
        d.zone,
        d.nb_docks,
        d.latitude,
        d.longitude,
        coalesce(d.total_departures, 0)  as total_departures,
        coalesce(a.total_arrivals,   0)  as total_arrivals,
        coalesce(d.peak_departures,  0)  as peak_departures,
        coalesce(a.peak_arrivals,    0)  as peak_arrivals,
        coalesce(d.avg_duration_mins, 0) as avg_duration_mins,

        -- Net flow: positive = draining (needs bikes), negative = filling (needs space)
        coalesce(d.total_departures, 0)
          - coalesce(a.total_arrivals, 0)  as net_flow,

        -- Utilisation: how busy is this station relative to its dock count?
        safe_divide(
            coalesce(d.total_departures, 0) + coalesce(a.total_arrivals, 0),
            d.nb_docks * 2.0
        )                                  as utilisation_rate,

        -- Imbalance score (0–1): how far off from balanced is this station?
        -- Uses absolute net flow normalised by total activity
        safe_divide(
            abs(coalesce(d.total_departures, 0) - coalesce(a.total_arrivals, 0)),
            greatest(
                coalesce(d.total_departures, 0) + coalesce(a.total_arrivals, 0),
                1
            )
        )                                  as imbalance_score

    from departures d
    left join arrivals a
        on d.hire_date = a.hire_date
        and d.station_id = a.station_id

)

select
    *,
    -- Flag as imbalanced if score > 0.2 (20% more flow one way than other)
    case when imbalance_score > 0.2 then true else false end as is_imbalanced,

    -- Direction of imbalance
    case
        when net_flow >  0 then 'draining'   -- needs bikes delivered
        when net_flow <  0 then 'filling'    -- needs bikes collected
        else                    'balanced'
    end as imbalance_direction

from joined

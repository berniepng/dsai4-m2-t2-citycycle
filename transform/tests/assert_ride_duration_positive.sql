-- tests/assert_ride_duration_positive.sql
-- ==========================================
-- Custom singular test: every ride must have end_datetime > start_datetime.
-- Returns rows that FAIL the assertion (dbt fails the test if any rows returned).

select
    rental_id,
    start_datetime,
    end_datetime,
    duration_seconds
from {{ ref('fact_rides') }}
where end_datetime <= start_datetime
   or duration_seconds <= 0

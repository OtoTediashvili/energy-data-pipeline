-- A zone's intervals must tile time: each one ends no later than the next one
-- starts. Two prices for overlapping periods mean two sources got mixed, as
-- when Austria's hourly main auction and EXAA's quarter-hours met on
-- 30 September 2025. Returns each overlap; zero rows means the test passes.

with ordered as (

    select
        bidding_zone,
        interval_start_utc,
        interval_end_utc,
        lead(interval_start_utc) over (
            partition by bidding_zone order by interval_start_utc
        ) as next_start_utc
    from {{ ref('stg_day_ahead_prices') }}

)

select *
from ordered
where next_start_utc < interval_end_utc

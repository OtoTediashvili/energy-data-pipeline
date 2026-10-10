-- Reconciliation. Staging collapses deliveries; it must never lose an interval.
-- Every distinct (bidding_zone, interval_start_utc) of the main auction in raw
-- must appear in staging. The uniqueness test covers the other half: none
-- appears twice. Returns the missing keys; zero rows means the test passes.

with raw_keys as (

    select distinct bidding_zone, interval_start_utc
    from {{ source('raw', 'day_ahead_prices') }}
    where coalesce(auction_sequence, 1) = 1

),

staged_keys as (

    select bidding_zone, interval_start_utc
    from {{ ref('stg_day_ahead_prices') }}

)

select r.bidding_zone, r.interval_start_utc
from raw_keys as r
left join staged_keys as s
    on r.bidding_zone = s.bidding_zone
    and r.interval_start_utc = s.interval_start_utc
where s.bidding_zone is null

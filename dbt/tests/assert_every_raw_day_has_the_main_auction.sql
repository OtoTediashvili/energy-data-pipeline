-- Staging keeps only the main auction. If ENTSO-E ever delivered a day with
-- only a second auction, that day would vanish from staging and from the
-- marts without a sound. Every (zone, market day) that raw holds in any
-- auction must reach staging. Returns the missing days; zero rows passes.

with raw_days as (

    select distinct
        bidding_zone,
        cast(
            (cast(interval_start_utc as timestamptz) at time zone '{{ var("market_timezone") }}')
            as date
        ) as market_date
    from {{ source('raw', 'day_ahead_prices') }}

),

staged_days as (

    select distinct bidding_zone, market_date
    from {{ ref('stg_day_ahead_prices') }}

)

select r.bidding_zone, r.market_date
from raw_days as r
left join staged_days as s
    on r.bidding_zone = s.bidding_zone
    and r.market_date = s.market_date
where s.bidding_zone is null

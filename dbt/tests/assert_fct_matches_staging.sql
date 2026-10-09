-- Reconciliation. The fact table is built incrementally, so a day can be
-- skipped without any other test noticing: every row present is still
-- correct, it is the missing or stale ones that are wrong. Rebuild the
-- expectation from staging in full and compare: every (zone, market day) must
-- exist on both sides, with the same number of intervals and the same latest
-- ingestion. Returns the mismatches; zero rows means the test passes.

with expected as (

    select
        z.bidding_zone_sk,
        s.market_date,
        count(*)            as interval_count,
        max(s.ingested_at)  as last_ingested_at
    from {{ ref('stg_day_ahead_prices') }} as s
    inner join {{ ref('dim_bidding_zone') }} as z
        on s.bidding_zone = z.bidding_zone_eic
    group by 1, 2

),

built as (

    select bidding_zone_sk, market_date, interval_count, last_ingested_at
    from {{ ref('fct_daily_prices') }}

)

select
    coalesce(e.bidding_zone_sk, b.bidding_zone_sk)  as bidding_zone_sk,
    coalesce(e.market_date, b.market_date)          as market_date,
    e.interval_count                                as expected_interval_count,
    b.interval_count                                as built_interval_count,
    e.last_ingested_at                              as expected_last_ingested_at,
    b.last_ingested_at                              as built_last_ingested_at
from expected as e
full outer join built as b
    on e.bidding_zone_sk = b.bidding_zone_sk
    and e.market_date = b.market_date
where e.market_date is null
    or b.market_date is null
    or e.interval_count is distinct from b.interval_count
    or e.last_ingested_at is distinct from b.last_ingested_at

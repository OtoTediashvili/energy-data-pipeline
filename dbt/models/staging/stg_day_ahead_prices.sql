-- Staging: keep the main auction, cast, derive the market day, and collapse
-- deliveries to one row per market time unit.
--
-- Some zones have two day-ahead auctions. Austria has the main European one
-- (SDAC, auction_sequence 1) and EXAA's separate 10:15 auction (2); zones with
-- one auction carry no sequence. Only the main auction is the day-ahead price
-- the market settles on, so staging keeps it alone. From October 2025 both are
-- quarter-hourly with identical timestamps: without this filter the dedup
-- below picked one of two prices per quarter-hour at random, and every test
-- passed. EXAA's rows stay in raw.
--
-- Raw is a delivery log. Consecutive runs overlap by one market day, and
-- ENTSO-E republishes corrections under a higher revisionNumber, so the same
-- quarter-hour can arrive several times. Keep exactly one: the highest
-- revision, then the latest ingestion, then the latest run.

with source as (

    select * from {{ source('raw', 'day_ahead_prices') }}

),

main_auction as (

    select * from source
    where coalesce(auction_sequence, 1) = 1

),

ranked as (

    select
        *,
        row_number() over (
            partition by bidding_zone, interval_start_utc
            order by revision_number desc, _ingested_at desc, _logical_date desc
        ) as delivery_rank
    from main_auction

)

select
    cast(bidding_zone as varchar)            as bidding_zone,
    cast(interval_start_utc as timestamptz)  as interval_start_utc,
    cast(interval_end_utc as timestamptz)    as interval_end_utc,
    -- The day-ahead auction defines a day in market time (CET/CEST), not UTC.
    -- A zone-aware conversion, never a fixed offset: +2 is right in summer and
    -- silently wrong for every winter hour.
    cast(
        (cast(interval_start_utc as timestamptz) at time zone '{{ var("market_timezone") }}')
        as date
    )                                        as market_date,
    cast(resolution_minutes as integer)      as resolution_minutes,
    cast(price_eur_mwh as double)            as price_eur_mwh,
    cast(currency as varchar)                as currency,
    cast(price_unit as varchar)              as price_unit,
    cast(is_filled as boolean)               as is_filled,
    cast(revision_number as integer)         as revision_number,
    cast(document_mrid as varchar)           as document_mrid,
    cast(_ingested_at as timestamp)          as ingested_at,
    cast(_logical_date as date)              as delivered_for_date
from ranked
where delivery_rank = 1

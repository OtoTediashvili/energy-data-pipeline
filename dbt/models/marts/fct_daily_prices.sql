-- Fact: day-ahead prices per bidding zone per market day.
--
-- Incremental, delete+insert on (bidding_zone_sk, market_date), so a rerun
-- replaces a day rather than duplicating it. Each incremental run rebuilds a
-- trailing window of market days, not only new ones: ENTSO-E republishes
-- corrections for days already built, and a correction arriving outside the
-- rebuilt window would never reach this table.
--
-- The window is anchored on the latest day already built here, not the latest
-- day in staging. After an outage staging jumps ahead by many days; a window
-- anchored on staging would cover only the newest few and leave a permanent
-- gap behind them. Anchored on this table, everything since the last build is
-- rebuilt, however long the outage.

{{
    config(
        materialized='incremental',
        unique_key=['bidding_zone_sk', 'market_date'],
        incremental_strategy='delete+insert'
    )
}}

with intervals as (

    select * from {{ ref('stg_day_ahead_prices') }}

    {% if is_incremental() %}
    where market_date >= (
        select coalesce(max(market_date), date '1900-01-01') from {{ this }}
    ) - {{ var('revision_lookback_days') }}
    {% endif %}

),

zones as (

    select bidding_zone_sk, bidding_zone_eic from {{ ref('dim_bidding_zone') }}

)

select
    z.bidding_zone_sk,
    i.market_date,
    count(*)                                        as interval_count,
    sum(i.resolution_minutes)                       as covered_minutes,
    count(*) filter (where i.is_filled)             as filled_interval_count,
    -- Time-weighted, so a day mixing 15- and 60-minute prices still averages
    -- correctly. With equal intervals it equals the plain mean.
    round(
        sum(i.price_eur_mwh * i.resolution_minutes) / sum(i.resolution_minutes), 4
    )                                               as avg_price_eur_mwh,
    min(i.price_eur_mwh)                            as min_price_eur_mwh,
    max(i.price_eur_mwh)                            as max_price_eur_mwh,
    count(*) filter (where i.price_eur_mwh < 0)     as negative_price_interval_count,
    max(i.revision_number)                          as max_revision_number,
    max(i.ingested_at)                              as last_ingested_at
from intervals as i
inner join zones as z
    on i.bidding_zone = z.bidding_zone_eic
group by 1, 2

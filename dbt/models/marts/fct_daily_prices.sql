-- Fact: day-ahead prices per bidding zone per market day.
--
-- Incremental, delete+insert on (bidding_zone_sk, market_date), so a rerun
-- replaces a day rather than duplicating it.
--
-- Which days to rebuild is decided by arrival time, not by market date: every
-- day that received a delivery since this table was last built. That covers
-- tomorrow's freshly published prices, a correction to last week, and a
-- backfill of a day from last year alike. An earlier version rebuilt a window
-- of market days behind the newest one built here; days loaded by a backfill
-- fell outside that window and never reached this table, while every test
-- still passed.
--
-- A day is always rebuilt from all of its intervals in staging, not only the
-- newly arrived ones, so a partly corrected day is aggregated whole.

{{
    config(
        materialized='incremental',
        unique_key=['bidding_zone_sk', 'market_date'],
        incremental_strategy='delete+insert'
    )
}}

with staged as (

    select * from {{ ref('stg_day_ahead_prices') }}

),

{% if is_incremental() %}

-- Staging keeps one delivery per interval, and a newer delivery wins it, so a
-- day changed exactly when one of its rows now carries a later ingestion time
-- than anything this table has seen.
changed_days as (

    select distinct bidding_zone, market_date
    from staged
    where ingested_at > (
        select coalesce(max(last_ingested_at), timestamp '1900-01-01') from {{ this }}
    )

),

{% endif %}

intervals as (

    select s.*
    from staged as s
    {% if is_incremental() %}
    inner join changed_days as c
        on s.bidding_zone = c.bidding_zone
        and s.market_date = c.market_date
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

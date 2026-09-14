-- Fact: one row per country per day, at the grain the business asks questions at.
--
-- Incremental so a daily run touches one partition rather than rebuilding
-- history. delete+insert, not append, so a rerun of a date is idempotent.

{{
    config(
        materialized='incremental',
        unique_key=['country_sk', 'observed_date'],
        incremental_strategy='delete+insert'
    )
}}

with prices as (

    select * from {{ ref('stg_prices') }}

    {% if is_incremental() %}
    where observed_date >= (select coalesce(max(observed_date), '1900-01-01') from {{ this }})
    {% endif %}

),

countries as (

    select * from {{ ref('dim_country') }}

)

select
    c.country_sk,
    p.observed_date,
    count(*)                              as observation_count,
    avg(p.price_eur_mwh)                  as avg_price_eur_mwh,
    min(p.price_eur_mwh)                  as min_price_eur_mwh,
    max(p.price_eur_mwh)                  as max_price_eur_mwh,
    count(p.price_eur_mwh)                as priced_observation_count,
    max(p.ingested_at)                    as last_ingested_at
from prices p
inner join countries c
    on p.country_code = c.country_code
group by 1, 2

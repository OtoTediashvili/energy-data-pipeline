-- Dimension: one row per country. Surrogate key so the fact table never
-- depends on a natural key the source might change.

with countries as (

    select distinct country_code
    from {{ ref('stg_prices') }}
    where country_code is not null

)

select
    {{ dbt_utils.generate_surrogate_key(['country_code']) }} as country_sk,
    country_code,
    case country_code
        when 'GE' then 'Georgia'
        when 'DE' then 'Germany'
        when 'FR' then 'France'
        when 'IT' then 'Italy'
        when 'ES' then 'Spain'
        when 'PL' then 'Poland'
        else 'Unknown'
    end as country_name
from countries

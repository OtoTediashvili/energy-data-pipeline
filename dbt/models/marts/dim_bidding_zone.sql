-- Dimension: one row per bidding zone present in the data, named from the
-- reference seed. A zone missing from the seed still gets a row, as
-- 'Unmapped', instead of silently vanishing from every join; a warning-level
-- test on staging flags it so the seed can be extended.

with zones as (

    select distinct bidding_zone from {{ ref('stg_day_ahead_prices') }}

),

reference as (

    select * from {{ ref('bidding_zones') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['z.bidding_zone']) }} as bidding_zone_sk,
    z.bidding_zone                                             as bidding_zone_eic,
    coalesce(r.bidding_zone_name, 'Unmapped')                  as bidding_zone_name,
    r.country_code
from zones as z
left join reference as r
    on z.bidding_zone = r.bidding_zone_eic

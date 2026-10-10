-- Within one delivery, an auction must give each interval one price.
--
-- ENTSO-E does send some series twice in one document: in 2026, every market
-- day for Belgium, the Czech Republic and Austria's main auction arrives as
-- two identical series. Identical copies are harmless; staging keeps one. Two
-- different prices for the same interval are not: staging would have no sound
-- way to choose, so they fail here. Returns each conflict; zero rows passes.

select
    _source_file,
    bidding_zone,
    auction_sequence,
    interval_start_utc,
    count(*)                        as copies,
    count(distinct price_eur_mwh)   as distinct_prices
from {{ source('raw', 'day_ahead_prices') }}
group by 1, 2, 3, 4
having count(distinct price_eur_mwh) > 1

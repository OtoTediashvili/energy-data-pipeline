-- Staging: rename, cast, deduplicate. No joins, no business logic.
--
-- The source publishes revisions: the same (id, date) can arrive more than
-- once with a corrected value. We keep the most recently ingested version.

with source as (

    select * from {{ source('raw', 'raw_prices') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by id, _logical_date
            order by _ingested_at desc
        ) as _revision_rank
    from source

),

renamed as (

    select
        cast(id as bigint)                as price_id,
        upper(trim(country))              as country_code,
        cast(value as double)             as price_eur_mwh,
        cast(_logical_date as date)       as observed_date,
        cast(_ingested_at as timestamp)   as ingested_at
    from deduplicated
    where _revision_rank = 1

)

select * from renamed

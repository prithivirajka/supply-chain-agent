-- Geolocation has multiple rows per zip prefix (different lat/lng readings).
-- We aggregate to a SINGLE row per zip_code_prefix only (not per city+zip)
-- so that joining to sellers/customers never fans out and creates duplicates.
with source as (
    select * from {{ source('olist', 'geolocation') }}
),

deduplicated as (
    select
        cast(geolocation_zip_code_prefix as varchar) as zip_code_prefix,
        avg(cast(geolocation_lat as double))         as latitude,
        avg(cast(geolocation_lng as double))         as longitude,
        -- take the most common city/state for this zip prefix
        mode() within group (order by lower(trim(geolocation_city)))  as city,
        mode() within group (order by upper(trim(geolocation_state))) as state
    from source
    group by geolocation_zip_code_prefix
)

select * from deduplicated

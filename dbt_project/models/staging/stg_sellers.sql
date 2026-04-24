with source as (
    select * from {{ source('olist', 'sellers') }}
),

geo as (
    select * from {{ ref('stg_geolocation') }}
),

renamed as (
    select
        -- keys
        s.seller_id,

        -- location (raw)
        cast(s.seller_zip_code_prefix as varchar) as zip_code_prefix,
        lower(trim(s.seller_city))                as seller_city,
        upper(trim(s.seller_state))               as seller_state,

        -- enriched coordinates from geolocation (joined on zip only — one row per zip)
        g.latitude  as seller_latitude,
        g.longitude as seller_longitude

    from source s
    left join geo g
        on cast(s.seller_zip_code_prefix as varchar) = g.zip_code_prefix
)

select * from renamed

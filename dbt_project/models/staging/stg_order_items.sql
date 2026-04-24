with source as (
    select * from {{ source('olist', 'order_items') }}
),

renamed as (
    select
        -- keys
        order_id,
        order_item_id,
        product_id,
        seller_id,

        -- dates
        cast(shipping_limit_date as timestamp) as shipping_limit_at,

        -- financials
        cast(price         as numeric) as item_price,
        cast(freight_value as numeric) as freight_value,

        -- derived
        cast(price as numeric) + cast(freight_value as numeric) as line_total

    from source
)

select * from renamed

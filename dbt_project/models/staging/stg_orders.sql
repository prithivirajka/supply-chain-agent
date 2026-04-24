with source as (
    select * from {{ source('olist', 'orders') }}
),

renamed as (
    select
        -- keys
        order_id,
        customer_id,

        -- status
        order_status,

        -- timestamps
        cast(order_purchase_timestamp      as timestamp) as purchased_at,
        cast(order_approved_at             as timestamp) as approved_at,
        cast(order_delivered_carrier_date  as timestamp) as shipped_at,
        cast(order_delivered_customer_date as timestamp) as delivered_at,
        cast(order_estimated_delivery_date as date)      as estimated_delivery_date,

        -- derived date (used for partitioning / grouping)
        cast(order_purchase_timestamp as date) as order_date,

        -- derived flags
        case
            when order_status = 'delivered'                      then true
            else false
        end as is_delivered,

        case
            when order_status in ('canceled', 'unavailable')     then true
            else false
        end as is_canceled

    from source
)

select * from renamed

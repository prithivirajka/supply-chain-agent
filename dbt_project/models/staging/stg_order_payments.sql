with source as (
    select * from {{ source('olist', 'order_payments') }}
),

renamed as (
    select
        -- keys
        order_id,
        payment_sequential,

        -- payment details
        payment_type,
        cast(payment_installments as integer) as payment_installments,
        cast(payment_value        as numeric)  as payment_value,

        -- derived: normalise "not_defined" to null for cleaner analytics
        case
            when payment_type = 'not_defined' then null
            else payment_type
        end as payment_type_clean,

        -- flag for instalment purchases (common in Brazilian e-commerce)
        case
            when cast(payment_installments as integer) > 1 then true
            else false
        end as is_installment_purchase

    from source
)

select * from renamed

with source as (
    select * from {{ source('olist', 'customers') }}
),

renamed as (
    select
        -- keys
        customer_id,           -- per-order key (not unique per person)
        customer_unique_id,    -- true customer identity

        -- location
        cast(customer_zip_code_prefix as varchar) as zip_code_prefix,
        lower(trim(customer_city))                as customer_city,
        upper(trim(customer_state))               as customer_state

    from source
)

select * from renamed

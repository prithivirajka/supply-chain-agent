with source as (
    select * from {{ source('olist', 'products') }}
),

translations as (
    select * from {{ source('olist', 'product_category_name_translation') }}
),

renamed as (
    select
        -- keys
        p.product_id,

        -- category (Portuguese + English)
        p.product_category_name,
        coalesce(t.product_category_name_english, p.product_category_name)
            as product_category_name_en,

        -- content metadata
        -- note: source CSV has typo 'lenght' instead of 'length' — mapped here
        cast(p.product_name_lenght        as integer) as product_name_length,
        cast(p.product_description_lenght as integer) as product_description_length,
        cast(p.product_photos_qty         as integer) as product_photos_qty,

        -- physical dimensions
        cast(p.product_weight_g  as numeric) as weight_g,
        cast(p.product_length_cm as numeric) as length_cm,
        cast(p.product_height_cm as numeric) as height_cm,
        cast(p.product_width_cm  as numeric) as width_cm,

        -- derived: volumetric weight proxy (length * height * width in cm3)
        cast(p.product_length_cm as numeric)
            * cast(p.product_height_cm as numeric)
            * cast(p.product_width_cm  as numeric) as volume_cm3

    from source p
    left join translations t
        on p.product_category_name = t.product_category_name
)

select * from renamed

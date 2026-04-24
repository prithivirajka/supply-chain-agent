with items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

orders as (
    select order_id, order_date, is_canceled, review_score, review_sentiment
    from {{ ref('int_orders_enriched') }}
    where is_canceled = false
),

product_sales as (
    select
        i.product_id,
        count(distinct i.order_id)                                              as total_orders,
        count(*)                                                                as total_units_sold,
        sum(i.item_price)                                                       as total_revenue,
        avg(i.item_price)                                                       as avg_item_price,
        sum(i.freight_value)                                                    as total_freight_collected,
        avg(i.freight_value)                                                    as avg_freight_value,

        case when sum(i.item_price) > 0
            then sum(i.freight_value)::double / sum(i.item_price)
            else null
        end                                                                     as freight_to_price_ratio,

        avg(o.review_score)                                                     as avg_review_score,
        count(case when o.review_sentiment = 'positive' then 1 end)            as positive_reviews,
        count(case when o.review_sentiment = 'negative' then 1 end)            as negative_reviews,
        min(o.order_date)                                                       as first_sold_date,
        max(o.order_date)                                                       as last_sold_date

    from items i
    left join orders o on i.order_id = o.order_id
    group by i.product_id
),

final as (
    select
        p.product_id,
        p.product_category_name,
        p.product_category_name_en,

        p.weight_g,
        p.length_cm,
        p.height_cm,
        p.width_cm,
        p.volume_cm3,
        p.product_photos_qty,
        p.product_name_length,
        p.product_description_length,

        ps.total_orders,
        ps.total_units_sold,
        ps.total_revenue,
        ps.avg_item_price,
        ps.total_freight_collected,
        ps.avg_freight_value,
        ps.freight_to_price_ratio,
        ps.avg_review_score,
        ps.positive_reviews,
        ps.negative_reviews,
        ps.first_sold_date,
        ps.last_sold_date

    from products p
    left join product_sales ps on p.product_id = ps.product_id
)

select * from final

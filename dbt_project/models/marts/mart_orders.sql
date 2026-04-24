-- Complete order fact table.
-- The primary table the agent queries for order-level questions.
-- Joins enriched orders with customer and product category context.

with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    select
        customer_id,
        customer_unique_id,
        customer_state,
        customer_city
    from {{ ref('stg_customers') }}
),

-- Get primary product category per order (category of highest-value item)
order_primary_category as (
    select distinct on (oi.order_id)
        oi.order_id,
        p.product_category_name_en as primary_category
    from {{ ref('stg_order_items') }} oi
    left join {{ ref('stg_products') }} p on oi.product_id = p.product_id
    order by oi.order_id, oi.item_price desc
),

final as (
    select
        -- keys
        o.order_id,
        o.customer_id,
        c.customer_unique_id,

        -- location
        c.customer_state,
        c.customer_city,

        -- order metadata
        o.order_status,
        o.order_date,
        date_trunc('month', o.order_date) as order_month,
        date_trunc('quarter', o.order_date) as order_quarter,
        extract(year from o.order_date)   as order_year,
        o.is_delivered,
        o.is_canceled,

        -- product context
        pc.primary_category,

        -- timestamps
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_date,

        -- items
        o.item_count,
        o.distinct_seller_count,
        o.distinct_product_count,

        -- financials
        o.items_subtotal,
        o.freight_total,
        o.order_gross_total,
        o.total_payment_value,

        -- payment behaviour
        o.payment_types_used,
        o.max_installments,
        o.has_installments,

        -- review
        o.review_score,
        o.review_sentiment,

        -- flags
        o.is_high_value_order

    from orders o
    left join customers         c  on o.customer_id = c.customer_id
    left join order_primary_category pc on o.order_id  = pc.order_id
)

select * from final

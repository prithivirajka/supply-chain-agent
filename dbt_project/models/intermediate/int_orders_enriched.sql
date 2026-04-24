-- One wide row per order, joining orders + items + payments + reviews.
-- Items and payments are aggregated to order level before joining,
-- so this remains a single row per order_id.

with orders as (
    select * from {{ ref('stg_orders') }}
),

-- Aggregate items to order level
items_agg as (
    select
        order_id,
        count(*)                        as item_count,
        sum(item_price)                 as items_subtotal,
        sum(freight_value)              as freight_total,
        sum(line_total)                 as order_gross_total,
        count(distinct seller_id)       as distinct_seller_count,
        count(distinct product_id)      as distinct_product_count,
        min(shipping_limit_at)          as earliest_shipping_limit_at
    from {{ ref('stg_order_items') }}
    group by order_id
),

-- Aggregate payments to order level (an order can have multiple payment methods)
payments_agg as (
    select
        order_id,
        sum(payment_value)                                              as total_payment_value,
        count(distinct payment_sequential)                              as payment_count,
        max(payment_installments)                                       as max_installments,
        bool_or(is_installment_purchase)                                as has_installments,
        -- Capture all payment types used as a comma-separated string
        string_agg(distinct payment_type_clean, ', ' order by payment_type_clean)
                                                                        as payment_types_used
    from {{ ref('stg_order_payments') }}
    group by order_id
),

-- Keep only the latest review per order (some orders have multiple)
reviews_latest as (
    select distinct on (order_id)
        order_id,
        review_id,
        review_score,
        review_sentiment,
        review_comment_title,
        review_comment_message,
        survey_sent_date,
        answered_at
    from {{ ref('stg_order_reviews') }}
    order by order_id, answered_at desc nulls last
),

joined as (
    select
        -- ── order core ────────────────────────────────────────────────
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_date,
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_date,
        o.is_delivered,
        o.is_canceled,

        -- ── items ─────────────────────────────────────────────────────
        coalesce(i.item_count,              0)    as item_count,
        coalesce(i.items_subtotal,          0)    as items_subtotal,
        coalesce(i.freight_total,           0)    as freight_total,
        coalesce(i.order_gross_total,       0)    as order_gross_total,
        coalesce(i.distinct_seller_count,   0)    as distinct_seller_count,
        coalesce(i.distinct_product_count,  0)    as distinct_product_count,
        i.earliest_shipping_limit_at,

        -- ── payments ──────────────────────────────────────────────────
        coalesce(p.total_payment_value,     0)    as total_payment_value,
        coalesce(p.payment_count,           0)    as payment_count,
        coalesce(p.max_installments,        1)    as max_installments,
        coalesce(p.has_installments,        false) as has_installments,
        p.payment_types_used,

        -- ── review ────────────────────────────────────────────────────
        r.review_id,
        r.review_score,
        r.review_sentiment,
        r.answered_at as review_answered_at,

        -- ── derived value flag ────────────────────────────────────────
        case
            when coalesce(i.order_gross_total, 0)
                 >= {{ var('high_value_order_threshold') }} then true
            else false
        end as is_high_value_order

    from orders o
    left join items_agg    i on o.order_id = i.order_id
    left join payments_agg p on o.order_id = p.order_id
    left join reviews_latest r on o.order_id = r.order_id
)

select * from joined

-- Delivery performance mart.
-- One row per delivered order. Includes all timing metrics,
-- late flags, seller compliance, and geographic context.

with delivery as (
    select * from {{ ref('int_delivery_performance') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_date,
        date_trunc('month', order_date)   as order_month,
        date_trunc('quarter', order_date) as order_quarter,
        extract(year from order_date)     as order_year,
        order_gross_total,
        item_count,
        distinct_seller_count
    from {{ ref('int_orders_enriched') }}
),

customers as (
    select customer_id, customer_unique_id, customer_state, customer_city
    from {{ ref('stg_customers') }}
),

-- Primary seller per order (seller of the highest-value item)
order_primary_seller as (
    select distinct on (oi.order_id)
        oi.order_id,
        oi.seller_id          as primary_seller_id,
        s.seller_state        as seller_state,
        s.seller_city         as seller_city
    from {{ ref('stg_order_items') }} oi
    left join {{ ref('stg_sellers') }} s on oi.seller_id = s.seller_id
    order by oi.order_id, oi.item_price desc
),

final as (
    select
        d.order_id,
        o.customer_id,
        c.customer_unique_id,
        c.customer_state,
        c.customer_city,
        ps.primary_seller_id,
        ps.seller_state,
        ps.seller_city,

        -- time context
        o.order_date,
        o.order_month,
        o.order_quarter,
        o.order_year,

        -- order size
        o.order_gross_total,
        o.item_count,
        o.distinct_seller_count,

        -- timing
        d.purchased_at,
        d.shipped_at,
        d.delivered_at,
        d.estimated_delivery_date,
        d.shipping_limit_at,

        d.approval_lag_days,
        d.days_to_ship,
        d.days_to_deliver,
        d.estimated_delivery_days,
        d.days_vs_estimate,

        -- performance
        d.seller_shipped_on_time,
        d.is_late,
        d.is_critical_delay,
        d.delivery_status

    from delivery d
    left join orders    o  on d.order_id = o.order_id
    left join customers c  on o.customer_id = c.customer_id
    left join order_primary_seller ps on d.order_id = ps.order_id
)

select * from final

-- Seller scorecard mart.
-- One row per seller. All aggregated KPIs the agent needs to answer
-- seller performance questions.

select
    seller_id,
    seller_city,
    seller_state,
    seller_latitude,
    seller_longitude,

    -- volume
    total_orders,
    total_items_sold,

    -- revenue
    total_item_revenue,
    total_freight_revenue,
    total_revenue,
    avg_order_revenue,

    -- review quality
    avg_review_score,
    positive_review_count,
    negative_review_count,
    reviewed_order_count,
    positive_review_rate,

    -- delivery performance
    delivered_order_count,
    late_delivery_count,
    critical_delay_count,
    late_delivery_rate,
    on_time_ship_count,
    on_time_ship_rate,
    avg_days_to_deliver,

    -- activity window
    first_order_date,
    last_order_date,

    -- composite score (simple weighted index for ranking)
    -- components: review quality (40%) + on-time delivery (40%) + volume bonus (20%)
    round(
        (coalesce(positive_review_rate, 0)  * 0.4)
      + ((1 - coalesce(late_delivery_rate, 0)) * 0.4)
      + (least(total_orders::float / 100.0, 1.0) * 0.2)
    , 4) as seller_score

from {{ ref('int_seller_performance') }}

with orders as (
    select * from {{ ref('int_orders_enriched') }}
    where is_canceled = false
),

delivery as (
    select * from {{ ref('int_delivery_performance') }}
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

order_items as (
    select
        order_id,
        seller_id,
        sum(item_price)    as seller_item_revenue,
        sum(freight_value) as seller_freight_revenue,
        sum(line_total)    as seller_total_revenue,
        count(*)           as seller_item_count
    from {{ ref('stg_order_items') }}
    group by order_id, seller_id
),

seller_orders as (
    select
        oi.seller_id,
        oi.order_id,
        oi.seller_item_revenue,
        oi.seller_freight_revenue,
        oi.seller_total_revenue,
        oi.seller_item_count,
        o.order_date,
        o.review_score,
        o.review_sentiment,
        o.is_delivered,
        d.is_late,
        d.is_critical_delay,
        d.days_to_deliver,
        d.seller_shipped_on_time
    from order_items oi
    left join orders   o on oi.order_id = o.order_id
    left join delivery d on oi.order_id = d.order_id
),

aggregated as (
    select
        seller_id,

        count(distinct order_id)                                            as total_orders,
        sum(seller_item_count)                                              as total_items_sold,

        sum(seller_item_revenue)                                            as total_item_revenue,
        sum(seller_freight_revenue)                                         as total_freight_revenue,
        sum(seller_total_revenue)                                           as total_revenue,
        avg(seller_total_revenue)                                           as avg_order_revenue,

        avg(review_score)                                                   as avg_review_score,
        count(case when review_sentiment = 'positive' then 1 end)          as positive_review_count,
        count(case when review_sentiment = 'negative' then 1 end)          as negative_review_count,
        count(case when review_score is not null then 1 end)               as reviewed_order_count,

        count(case when is_late = true then 1 end)                         as late_delivery_count,
        count(case when is_critical_delay = true then 1 end)               as critical_delay_count,
        count(case when is_delivered = true then 1 end)                    as delivered_order_count,
        count(case when seller_shipped_on_time = true then 1 end)          as on_time_ship_count,
        avg(days_to_deliver)                                               as avg_days_to_deliver,

        min(order_date)                                                    as first_order_date,
        max(order_date)                                                    as last_order_date

    from seller_orders
    group by seller_id
),

final as (
    select
        a.*,
        s.seller_city,
        s.seller_state,
        s.seller_latitude,
        s.seller_longitude,

        -- safe divide: avoid division by zero using nullif
        case when a.delivered_order_count > 0
            then a.late_delivery_count::double / a.delivered_order_count
            else null
        end as late_delivery_rate,

        case when a.reviewed_order_count > 0
            then a.positive_review_count::double / a.reviewed_order_count
            else null
        end as positive_review_rate,

        case when a.total_orders > 0
            then a.on_time_ship_count::double / a.total_orders
            else null
        end as on_time_ship_rate

    from aggregated a
    left join sellers s on a.seller_id = s.seller_id
)

select * from final

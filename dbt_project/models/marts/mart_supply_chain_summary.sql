with orders as (
    select * from {{ ref('mart_orders') }}
),

delivery as (
    select * from {{ ref('mart_delivery') }}
),

sellers as (
    select * from {{ ref('mart_sellers') }}
),

monthly_orders as (
    select
        order_month,
        order_year,
        order_quarter,

        count(distinct order_id)                                                as total_orders,
        count(distinct case when is_delivered then order_id end)                as delivered_orders,
        count(distinct case when is_canceled  then order_id end)                as canceled_orders,
        count(distinct customer_unique_id)                                      as unique_customers,

        sum(order_gross_total)                                                  as gross_revenue,
        avg(order_gross_total)                                                  as avg_order_value,
        sum(freight_total)                                                      as total_freight,

        case when sum(order_gross_total) > 0
            then sum(freight_total)::double / sum(order_gross_total)
            else null
        end                                                                     as freight_revenue_ratio,

        avg(review_score)                                                       as avg_review_score,
        count(case when review_sentiment = 'positive' then 1 end)              as positive_reviews,
        count(case when review_sentiment = 'negative' then 1 end)              as negative_reviews,

        sum(item_count)                                                         as total_items_sold,
        count(distinct primary_category)                                        as active_categories

    from orders
    where order_month is not null
    group by order_month, order_year, order_quarter
),

monthly_delivery as (
    select
        order_month,
        count(*)                                                                as delivered_count,
        avg(days_to_deliver)                                                    as avg_days_to_deliver,
        avg(days_vs_estimate)                                                   as avg_days_vs_estimate,
        count(case when is_late           then 1 end)                          as late_deliveries,
        count(case when is_critical_delay then 1 end)                          as critical_delays,

        case when count(*) > 0
            then count(case when is_late then 1 end)::double / count(*)
            else null
        end                                                                     as late_delivery_rate,

        case when count(*) > 0
            then count(case when seller_shipped_on_time then 1 end)::double / count(*)
            else null
        end                                                                     as on_time_ship_rate

    from delivery
    where order_month is not null
    group by order_month
),

seller_snapshot as (
    select
        count(*)                                                                as total_active_sellers,
        avg(avg_review_score)                                                   as avg_seller_review_score,
        count(case when avg_review_score >= 4 then 1 end)                      as high_rated_sellers,
        count(case when late_delivery_rate  <= 0.1 then 1 end)                 as reliable_sellers
    from sellers
),

final as (
    select
        mo.order_month,
        mo.order_year,
        mo.order_quarter,

        mo.total_orders,
        mo.delivered_orders,
        mo.canceled_orders,
        case when mo.total_orders > 0
            then mo.canceled_orders::double / mo.total_orders
            else null
        end                                                                     as cancellation_rate,

        mo.unique_customers,
        mo.gross_revenue,
        mo.avg_order_value,
        mo.total_freight,
        mo.freight_revenue_ratio,
        mo.total_items_sold,
        mo.active_categories,

        mo.avg_review_score,
        mo.positive_reviews,
        mo.negative_reviews,
        case when (mo.positive_reviews + mo.negative_reviews) > 0
            then mo.positive_reviews::double / (mo.positive_reviews + mo.negative_reviews)
            else null
        end                                                                     as positive_review_rate,

        md.avg_days_to_deliver,
        md.avg_days_vs_estimate,
        md.late_delivery_rate,
        md.on_time_ship_rate,
        md.late_deliveries,
        md.critical_delays,

        ss.total_active_sellers,
        ss.avg_seller_review_score,
        ss.high_rated_sellers,
        ss.reliable_sellers

    from monthly_orders  mo
    left join monthly_delivery md on mo.order_month = md.order_month
    cross join seller_snapshot  ss
    order by mo.order_month
)

select * from final

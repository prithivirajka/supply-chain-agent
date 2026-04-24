with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
    where is_canceled = false
),

order_to_unique_customer as (
    select
        o.*,
        c.customer_unique_id,
        c.customer_state,
        c.customer_city,
        c.zip_code_prefix
    from orders o
    left join customers c on o.customer_id = c.customer_id
),

aggregated as (
    select
        customer_unique_id,

        max(customer_state) as customer_state,
        max(customer_city)  as customer_city,

        count(distinct order_id)                                        as total_orders,
        count(distinct case when is_delivered then order_id end)        as delivered_orders,

        sum(order_gross_total)                                          as lifetime_gross_value,
        sum(total_payment_value)                                        as lifetime_payment_value,
        avg(order_gross_total)                                          as avg_order_value,

        avg(review_score)                                               as avg_review_score,
        count(case when review_sentiment = 'positive' then 1 end)       as positive_reviews,
        count(case when review_sentiment = 'negative' then 1 end)       as negative_reviews,

        min(order_date)                                                 as first_order_date,
        max(order_date)                                                 as last_order_date,
        datediff('day', min(order_date), max(order_date))               as customer_lifespan_days

    from order_to_unique_customer
    group by customer_unique_id
),

final as (
    select
        *,
        case when total_orders > 1 then true else false end as is_repeat_customer,

        case
            when last_order_date >= current_date - interval '3 months'  then 'active'
            when last_order_date >= current_date - interval '12 months' then 'lapsed'
            else 'churned'
        end as recency_segment

    from aggregated
)

select * from final

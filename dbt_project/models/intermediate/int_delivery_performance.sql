with orders as (
    select
        order_id,
        order_date,
        purchased_at,
        approved_at,
        shipped_at,
        delivered_at,
        estimated_delivery_date,
        is_delivered,
        is_canceled
    from {{ ref('stg_orders') }}
    where is_delivered = true
),

items as (
    select
        order_id,
        min(shipping_limit_at) as shipping_limit_at
    from {{ ref('stg_order_items') }}
    group by order_id
),

customers as (
    select customer_id, customer_state
    from {{ ref('stg_customers') }}
),

orders_with_customers as (
    select
        o.*,
        c.customer_state
    from orders o
    left join customers c on o.order_id = (
        select so.order_id
        from {{ ref('stg_orders') }} so
        where so.order_id = o.order_id
        limit 1
    )
),

delivery_calc as (
    select
        o.order_id,
        o.order_date,
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_date,
        i.shipping_limit_at,

        -- timing durations (in days)
        datediff('day', o.purchased_at, o.approved_at)
            as approval_lag_days,

        datediff('day', o.purchased_at, o.shipped_at)
            as days_to_ship,

        datediff('day', o.purchased_at, o.delivered_at)
            as days_to_deliver,

        datediff('day', o.purchased_at, cast(o.estimated_delivery_date as timestamp))
            as estimated_delivery_days,

        -- positive = early, negative = late
        datediff('day', o.delivered_at, cast(o.estimated_delivery_date as timestamp))
            as days_vs_estimate,

        -- seller shipping compliance
        case
            when o.shipped_at <= i.shipping_limit_at then true
            else false
        end as seller_shipped_on_time,

        -- late flags
        case
            when datediff('day', o.delivered_at, cast(o.estimated_delivery_date as timestamp))
                 < {{ var('late_delivery_threshold_days') }} then true
            else false
        end as is_late,

        case
            when datediff('day', o.delivered_at, cast(o.estimated_delivery_date as timestamp))
                 < (-1 * {{ var('critical_delay_threshold_days') }}) then true
            else false
        end as is_critical_delay

    from orders o
    left join items i on o.order_id = i.order_id
),

final as (
    select
        *,
        case
            when is_critical_delay                  then 'critical'
            when is_late                            then 'late'
            when days_vs_estimate between 0 and 2  then 'on_time'
            else                                        'early'
        end as delivery_status
    from delivery_calc
)

select * from final

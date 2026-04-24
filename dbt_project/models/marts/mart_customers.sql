-- Customer analytics mart.
-- One row per customer_unique_id. Combines order history, LTV,
-- review behaviour, recency segmentation, and geographic context.

with customers as (
    select * from {{ ref('int_customer_orders') }}
),

-- Pull most recent order's city/state for location context
customer_location as (
    select distinct on (c.customer_unique_id)
        c.customer_unique_id,
        c.customer_state,
        c.customer_city,
        c.zip_code_prefix
    from {{ ref('stg_customers') }} c
    inner join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
    order by c.customer_unique_id, o.purchased_at desc
),

final as (
    select
        c.customer_unique_id,

        -- location
        l.customer_state,
        l.customer_city,
        l.zip_code_prefix,

        -- order volume
        c.total_orders,
        c.delivered_orders,

        -- financials / LTV
        c.lifetime_gross_value,
        c.lifetime_payment_value,
        c.avg_order_value,

        -- review behaviour
        c.avg_review_score,
        c.positive_reviews,
        c.negative_reviews,

        -- activity window
        c.first_order_date,
        c.last_order_date,
        c.customer_lifespan_days,

        -- segmentation
        c.is_repeat_customer,
        c.recency_segment,

        -- value tier (based on lifetime_gross_value quartiles — approximate buckets)
        case
            when c.lifetime_gross_value >= 1000 then 'high'
            when c.lifetime_gross_value >= 300  then 'mid'
            else                                     'low'
        end as value_tier,

        -- engagement score: combines recency, frequency, and review quality
        round(
            -- recency (33%): active=1, lapsed=0.5, churned=0
            case c.recency_segment
                when 'active'  then 0.33
                when 'lapsed'  then 0.165
                else                0.0
            end
            -- frequency (33%): capped at 5 orders = full score
          + (least(c.total_orders::float / 5.0, 1.0) * 0.33)
            -- satisfaction (34%): avg_review_score normalised to 0–1
          + (coalesce((c.avg_review_score - 1) / 4.0, 0) * 0.34)
        , 4) as customer_engagement_score

    from customers c
    left join customer_location l on c.customer_unique_id = l.customer_unique_id
)

select * from final

with source as (
    select * from {{ source('olist', 'order_reviews') }}
),

renamed as (
    select
        -- keys
        review_id,
        order_id,

        -- score
        cast(review_score as integer) as review_score,

        -- derived sentiment bucket using project variables
        case
            when cast(review_score as integer) >= {{ var('positive_review_min_score') }} then 'positive'
            when cast(review_score as integer) <= {{ var('negative_review_max_score') }} then 'negative'
            else 'neutral'
        end as review_sentiment,

        -- text (may be null — many customers skip the comment)
        review_comment_title,
        review_comment_message,

        -- timestamps
        cast(review_creation_date    as date)      as survey_sent_date,
        cast(review_answer_timestamp as timestamp) as answered_at

    from source
)

select * from renamed

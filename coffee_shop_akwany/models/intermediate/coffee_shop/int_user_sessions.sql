with pageviews as (
    select * from {{ ref('int_user_stitching') }}  -- Use the stitched visitor_id
),

sessionized as (
    select 
        id,
        visitor_id,
        device_type,
        timestamp,
        page,
        customer_id,

        -- Identify the time difference from the previous pageview
        timestamp_diff(timestamp, lag(timestamp) over (
            partition by visitor_id 
            order by timestamp
        ), minute) as time_since_last_pageview,

        -- Define a session boundary (if gap > 30 min, start a new session)
        case 
            when timestamp_diff(timestamp, lag(timestamp) over (
                partition by visitor_id 
                order by timestamp
            ), minute) > 30 
            or lag(timestamp) over (
                partition by visitor_id 
                order by timestamp
            ) is null 
            then 1 
            else 0 
        end as new_session_flag

    from pageviews
),

final as (
    select 
        *,
        -- Generate a session_id by taking the cumulative sum of session boundaries
        concat(visitor_id, '_', cast(sum(new_session_flag) over (
            partition by visitor_id 
            order by timestamp 
            rows between unbounded preceding and current row
        ) as string)) as session_id

    from sessionized
)

select * from final

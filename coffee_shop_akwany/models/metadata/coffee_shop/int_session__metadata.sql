with sessions as (
    select * from {{ ref('int_user_sessions') }}
),

session_summary as (
    select 
        session_id,
        visitor_id,
        min(session_start_time) as session_start_time,
        max(session_end_time) as session_end_time,
        timestamp_diff(max(session_end_time), min(session_start_time), minute) as session_length,
        count(*) as num_pages_visited,
        max(case when page = 'purchase_confirmation' then 1 else 0 end) as purchase_made
    from sessions
    group by 1, 2
)

select * from session_summary

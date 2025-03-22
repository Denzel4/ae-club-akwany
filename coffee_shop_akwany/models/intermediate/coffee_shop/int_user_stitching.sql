with pageviews as (
    select * 
    from {{ source('web_tracking', 'pageviews') }}
),

customer_stitching as (
    -- Assign a single visitor_id per customer_id using array_agg()
    select 
        customer_id,
        array_agg(visitor_id order by visitor_id limit 1)[safe_offset(0)] as stitched_visitor_id
    from pageviews
    where customer_id is not null
    group by customer_id
)

select 
    distinct p.id,
    coalesce(cs.stitched_visitor_id, p.visitor_id) as visitor_id, -- Use stitched visitor_id when available
    p.device_type,
    p.timestamp,
    p.page,
    p.customer_id
from pageviews p
left join customer_stitching cs 
    on p.customer_id = cs.customer_id

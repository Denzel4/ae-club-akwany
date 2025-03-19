with pageviews as (
    select  * from {{ source('web_tracking','pageviews') }}
),

customer_stitching as (
    -- a single visitor_id per customer_id
    select 
        customer_id,
        min(visitor_id) as stitched_visitor_id
    from pageviews
    where customer_id is not null
    group by customer_id
)

select 
    p.id,
    coalesce(cs.stitched_visitor_id, p.visitor_id) as visitor_id, -- Use stitched visitor_id when available
    p.device_type,
    p.timestamp,
    p.page,
    p.customer_id
from pageviews p
left join customer_stitching cs 
    on p.customer_id = cs.customer_id

with first_order as (
select 
    min(order_date) as first_order_at,
    customer_id
from {{ ref('users') }}
    group by customer_id
)

select
    date_trunc(first_order_at, month) as signup_month,
    count(*) as new_customers
 
from first_order
 
group by 1
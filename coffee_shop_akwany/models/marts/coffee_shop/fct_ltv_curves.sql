with orders as (
    select
        customer_id,
        date(created_at) as order_date,  -- Fix: Replacing order_date with created_at
        total as revenue
    from {{ source('coffee_shop', 'orders') }}
    where customer_id is not null
),

customer_acquisition as (
    select
        customer_id,
        min(order_date) as acquisition_date
    from orders
    group by customer_id
),

ltv_curves as (
    select
        o.customer_id,
        date_diff(o.order_date, ca.acquisition_date, week) + 1 as week, -- Week starts at 1
        o.revenue,
        sum(o.revenue) over (
            partition by o.customer_id 
            order by o.order_date 
            rows between unbounded preceding and current row
        ) as cumulative_revenue
    from orders o
    join customer_acquisition ca 
        on o.customer_id = ca.customer_id
)

select * from ltv_curves

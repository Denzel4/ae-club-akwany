SELECT 

customer.id as customer_id,
orders.created_at as order_date,
--sum(orders.total) as total_orders,
customer.name,
customer.email

 FROM `analytics-engineers-club.coffee_shop.customers` as customer
 left join `analytics-engineers-club.coffee_shop.orders` as orders 
 on customer.id =orders.customer_id
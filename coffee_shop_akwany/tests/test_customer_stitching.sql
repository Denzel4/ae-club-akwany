-- Ensure No Customer Has Multiple Visitor IDs
SELECT customer_id
FROM {{ ref('int_user_stitching') }}  
GROUP BY customer_id
HAVING COUNT(DISTINCT visitor_id) > 1

select c.*, ck.kitCount 
from {{ ref('unified_customers') }} c
left join {{ ref('customer_kitCount') }} ck 
on c.shopify_customerId = ck.shopify_customerId

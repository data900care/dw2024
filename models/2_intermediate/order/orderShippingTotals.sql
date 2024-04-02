select shopify_orderId, 
sum(price) as totalShipping 
from {{ ref('stg_shopify__order_shipping_line') }}
group by shopify_orderId
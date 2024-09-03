select shopify_orderId, 
max(title) as shippingTitle
from {{ ref('stg_shopify__order_shipping_line') }}
group by shopify_orderId
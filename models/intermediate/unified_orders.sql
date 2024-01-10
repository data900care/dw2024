select *
from {{ ref('stg_shopify__orders') }} so
left join  {{ ref('stg_recharge__orders') }} ro
on so.shopify_orderId = external_order_id_ecommerce
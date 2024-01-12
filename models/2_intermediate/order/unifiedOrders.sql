
select so.*, 
   ro.isDeleted,
    ro.scheduledAt,
    ro.shippedAt
from {{ ref('validShopifyOrders') }} so
left join  {{ ref('stg_recharge__orders') }} ro
on so.shopify_orderId = external_order_id_ecommerce

select so.*, 
   ro.isDeleted,
   ro.createdAt as recharge_createdAt,
    ro.scheduledAt,
    ro.shippedAt
from {{ ref('validShopifyOrders') }} so
left join  {{ ref('stg_recharge__orders') }} ro
on so.shopify_orderId = ro.shopify_orderId
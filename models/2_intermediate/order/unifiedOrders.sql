
select so.*, 
   ro.isDeleted,
   ro.createdAt as recharge_createdAt,
    ro.scheduledAt,
    ro.shippedAt,
   coalesce(c2020.CustomerType, c2023.CustomerType) as orderCustomerType

from {{ ref('validShopifyOrders') }} so
left join  {{ ref('stg_recharge__orders') }} ro
using (shopify_orderId)
left join  {{ ref('orderCustomerType2020') }} c2020
using (shopify_orderId)
left join {{ ref('orderCustomerType20220117') }} c2023
using (shopify_orderId)

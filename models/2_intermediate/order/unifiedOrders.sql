
select so.*, 
   ro.isDeleted,
   ro.createdAt as recharge_createdAt,
    ro.scheduledAt,
    ro.shippedAt,
    --c2020.CustomerType as orderCustomerType2020,
    --c2023.CustomerType as     orderCustomerType2023,
    coalesce(c2020.CustomerType, c2023.CustomerType) as orderCustomerType,
    utm.data as utmCampaign,
    itemCount,
    totalItemPrice,
    totalItemQuantity

from {{ ref('validShopifyOrders') }} so
left join  {{ ref('stg_recharge__orders') }} ro
using (shopify_orderId)
left join  {{ ref('orderCustomerType2020') }} c2020
using (shopify_orderId)
left join {{ ref('orderCustomerType20220117') }} c2023
using (shopify_orderId)
left join {{ ref('stg_shopify__orderUTMCampaign') }} utm 
using (shopify_orderId)
left join {{ ref('orderLineAggregations') }} a 
using (shopify_orderId)
--where shopify_orderId = 4632692785231
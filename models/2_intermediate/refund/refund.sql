select 
 shopify_orderId,
 createdAt,
 amount
 from {{ ref('stg_shopify__refundTransactions') }} r 
 
where shopify_orderId  in (select shopify_orderId from  {{ ref('validShopifyOrders') }})

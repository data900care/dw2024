select 
 shopify_orderId,
 createdAt,
 amount
 from {{ ref('inner_shopify__refundTransactions') }} r 
 

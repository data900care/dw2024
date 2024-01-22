select 
 shopify_orderId,
 createdAt,
 subtotal
 from {{ ref('stg_shopify__refund') }} r 
 join {{ ref('stg_shopify__order_line_refund') }} rl
 using (refundId)
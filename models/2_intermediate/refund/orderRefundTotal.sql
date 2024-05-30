select shopify_orderId, sum(amount) as totalRefund
 from {{ ref("inner_shopify_refund_transaction") }} t
group by shopify_orderId

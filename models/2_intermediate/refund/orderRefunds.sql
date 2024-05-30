select t.*, newOrderCustomerType
 from {{ ref("inner_shopify_refund_transaction") }} t
join {{ ref('shopifyOrderL') }} o
using (shopify_orderId)

select t.*, newOrderCustomerType
 from {{ ref("stg_shopify__transaction") }} t
join {{ ref('shopifyOrderL') }} o
using (shopify_orderId)
where kind = 'refund'

select invalidLabel,o.* from {{ ref('shopifyOrderL') }} o
join {{ ref('init_invalidShopifyOrders') }} i
using(shopify_orderId)
where validorder = false
SELECT
    shopify_customerId,
    kitType
  FROM
   {{ ref('stg_shopify__order_kitTypes') }} k
   join {{ ref('validOrders') }} o
   on o.shopify_orderId = k.shopify_orderId
   where
    customerOrderRank = 1

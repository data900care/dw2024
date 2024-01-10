SELECT
    shopify_customerId,
    orderDiscountCode AS firstorder_orderDiscountCode
  FROM
   {{ ref('stg_shopify__orderDiscountCodes') }} dc
   join {{ ref('validOrders') }} o
   on o.shopify_orderId = dc.shopify_orderId
   where
    customerOrderRank = 1

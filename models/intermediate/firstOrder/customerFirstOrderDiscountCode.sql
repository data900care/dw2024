SELECT
    shopify_customerId,
    discountCode AS firstorder_DiscountCode
  FROM
   {{ ref('stg_shopify__orderDiscount') }} od
   join {{ ref('validOrders') }} o
   on o.shopify_orderId = od.shopify_orderId
   where
    customerOrderRank = 1

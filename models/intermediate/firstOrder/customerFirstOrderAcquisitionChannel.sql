SELECT
    shopify_customerId,
    acquisitionChannel AS firstorder_acquisitionChannel
  FROM
   {{ ref('order_acquisition_channel') }} oac
   join {{ ref('validOrders') }} o
   on o.shopify_orderId = oac.shopify_orderId
   where
    customerOrderRank = 1

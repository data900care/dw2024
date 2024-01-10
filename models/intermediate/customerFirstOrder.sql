SELECT
    shopify_customerId,
    shipping_address_country AS firstorder_shipping_country
  FROM
   {{ ref('validOrders') }}
   where customerOrderRank = 1

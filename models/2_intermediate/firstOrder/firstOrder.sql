SELECT
    shopify_customerId,
    discountCode AS firstOrder_discountCode,
    shipping_address_country AS firstOrder_shippingCountry,
    acquisitionChannel as firstorder_acquisitionchannel    
  FROM
 {{ ref('validShopifyOrders') }} o

   where
    customerOrderRank = 1

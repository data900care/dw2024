SELECT
    shopify_customerId,
    discountCode AS firstOrder_discountCode,
    shippingCountry AS firstOrder_shippingCountry,
    acquisitionChannel as firstOrder_acquisitionChannel ,
    createdAt as firstOrder_shopifyCreatedAt
  FROM
 {{ ref('validShopifyOrders') }} o

   where
    customerOrderRank = 1

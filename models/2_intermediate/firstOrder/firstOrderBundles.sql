SELECT
    shopify_customerId,
    bundleType
  FROM
   {{ ref('stg_shopify__orderBundles') }} ok
   join {{ ref('validShopifyOrders') }} o using (shopify_orderId)
   where
    customerOrderRank = 1

SELECT
    shopify_customerId,ok.shopify_orderId,
    bundleType
  FROM
   {{ ref('inner_shopify__orderBundles') }} ok
   join {{ ref('validShopifyOrders') }} o using (shopify_orderId)

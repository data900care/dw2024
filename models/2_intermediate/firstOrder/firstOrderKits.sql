SELECT
    shopify_customerId,
    kitType
  FROM
   {{ ref('stg_shopify__orderKits') }} ok
   join {{ ref('validShopifyOrders') }} o using (shopify_orderId)
   where
    customerOrderRank = 1

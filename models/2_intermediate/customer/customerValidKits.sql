SELECT
    shopify_customerId,ok.shopify_orderId,
    kitType
  FROM
   {{ ref('stg_shopify__orderKits') }} ok
   join {{ ref('validShopifyOrders') }} o using (shopify_orderId)

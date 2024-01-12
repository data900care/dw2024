SELECT
    distinct shopify_customerId,
    kitType
  FROM
   {{ ref('stg_shopify__orderKitTypes') }} ok
   join {{ ref('validShopifyOrders') }} o
   on o.shopify_orderId = ok.shopify_orderId

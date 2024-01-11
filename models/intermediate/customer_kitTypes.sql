SELECT
    shopify_customerId,
    kitType
  FROM
   {{ ref('stg_shopify__orderKitTypes') }} ok
   join {{ ref('validOrders') }} o
   on o.shopify_orderId = ok.shopify_orderId

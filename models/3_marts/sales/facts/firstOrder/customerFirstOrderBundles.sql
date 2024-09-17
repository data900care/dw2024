SELECT
    shopify_customerId,
    bundleType
  FROM
 {{ ref("orderBundles_from_order_note_attribute") }} ok
   join {{ ref("inner_shopify__order") }}  o using (shopify_orderId)
   where
    customerOrderRank = 1

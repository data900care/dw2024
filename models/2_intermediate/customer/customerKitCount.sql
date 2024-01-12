SELECT
    shopify_customerId,
    kitType,
    count(*) as kitCount
  FROM
   {{ ref('customerKitTypes') }}
group by shopify_customerId, kitType

SELECT
    shopify_customerId,
    kitType,
    count(*) as kitCount
  FROM
   {{ ref('customer_kitTypes') }}
group by shopify_customerId, kitType

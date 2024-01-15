select distinct 
    shopify_customerId,
    kitType
  FROM {{ ref('customerValidKits') }}
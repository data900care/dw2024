select distinct 
    shopify_customerId,
    bundleType
  FROM {{ ref('customerValidBundles') }}
select shopify_customerId,k.property, k.propertyValue
 from {{ ref('stg_klaviyo__profiles') }} k 
join {{ ref('stg_shopify__customer') }} c on k.email = c.email
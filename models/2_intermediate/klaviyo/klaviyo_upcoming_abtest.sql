select shopify_customerId,k.upcoming_abtest
 from {{ ref('stg_klaviyo__profiles_upcoming_abtest') }} k 
join {{ ref('stg_shopify__customer') }} c on k.email = c.email
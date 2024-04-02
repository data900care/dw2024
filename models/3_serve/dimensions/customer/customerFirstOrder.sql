select
   c.shopify_customerId, fo.* except(shopify_customerId)

from {{ ref('customerM') }} c
left join  {{ ref("firstOrder") }} fo using (shopify_customerId)

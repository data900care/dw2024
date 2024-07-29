select
   c.shopify_customerId, fo.* except(shopify_customerId)

from {{ ref('customerM') }} c
left join  {{ ref("firstOrder") }} fo using (shopify_customerId)
where exists (select 1  from {{ ref('shopifyOrderM') }} o where o.shopify_customerId = c.shopify_customerId and validOrder is true)

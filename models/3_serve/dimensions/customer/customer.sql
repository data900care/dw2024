select * except (email) from {{ ref('customerM') }} c
where exists (select 1  from {{ ref('shopifyOrderM') }} o where o.shopify_customerId = c.shopify_customerId and validOrder is true)
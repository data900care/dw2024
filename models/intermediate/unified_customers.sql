select *
from {{ ref('stg_shopify__customers') }} sc
left join  {{ ref('stg_recharge__customers') }} rc
on sc.shopify_customerId = external_customer_id_ecommerce

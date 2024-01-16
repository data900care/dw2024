select o.* ,
rank() over (partition by shopify_customerId order by o.shopify_orderId ) as customerOrderRank,
d.discountCode,
oac.acquisitionChannel

from {{ ref('stg_shopify__orders') }} o

left join {{ ref('stg_shopify__orderDiscount') }} d
on o.shopify_orderId = d.shopify_orderId

left join    {{ ref('orderAcquisitionChannel') }} oac
   on o.shopify_orderId = oac.shopify_orderId

where o.shopify_orderId not in 
(
    select shopify_orderId from {{ ref('invalidShopifyOrders') }}
)
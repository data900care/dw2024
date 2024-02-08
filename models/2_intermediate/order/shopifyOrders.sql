select o.shopify_orderId,
        billCountry,
        cancelReason,
        cancelledAt,
        cancelled,
        confirmed,
        createdAt,
        currency,
        shopify_customerId,
        shippingCountry,

        subTotal,
        discount,
        tax,
        taxesIncluded,
        totalShipping as shipping,
        total ,

rank() over (partition by shopify_customerId order by o.shopify_orderId ) as customerOrderRank,
d.discountCode,
oac.acquisitionChannel,
ifnull(orderWithTrialKit, false) as orderWithTrialKit,
true as validOrder

from {{ ref('stg_shopify__orders') }} o

left join {{ ref('orderShippingTotals') }} os
    using (shopify_orderId)

left join {{ ref('stg_shopify__orderDiscount') }} d
    using (shopify_orderId)

left join    {{ ref('orderAcquisitionChannel') }} oac
    using (shopify_orderId)

left join {{ ref('ordersWithTrialKit') }} owt 
    using (shopify_orderId)

where o.shopify_orderId not in 
(
    select shopify_orderId from {{ ref('invalidShopifyOrders') }}
)

union all

select o.shopify_orderId,
        billCountry,
        cancelReason,
        cancelledAt,
        cancelled,
        confirmed,
        createdAt,
        currency,
        shopify_customerId,
        shippingCountry,

        subTotal,
        discount,
        tax,
        taxesIncluded,
        totalShipping as shipping,
        total ,
-1 as customerOrderRank,
d.discountCode,
oac.acquisitionChannel,
ifnull(orderWithTrialKit, false) as orderWithTrialKit,
false as validOrder

from {{ ref('stg_shopify__orders') }} o

left join {{ ref('orderShippingTotals') }} os
    using (shopify_orderId)

left join {{ ref('stg_shopify__orderDiscount') }} d
    using (shopify_orderId)

left join    {{ ref('orderAcquisitionChannel') }} oac
    using (shopify_orderId)

left join {{ ref('ordersWithTrialKit') }} owt 
    using (shopify_orderId)

where o.shopify_orderId in 
(
    select shopify_orderId from {{ ref('invalidShopifyOrders') }}
)

select
    c.*,
    ck.kitcount,
    fo.firstorder_acquisitionChannel,
    fo.firstorder_discountCode,
    fo.firstorder_shippingCountry,
    cs.firstSubscriptionDate,
    cs.lastSubscriptionCancelledAt
from {{ ref("stg_shopify__customers") }} c
left join
    {{ ref("customerKitCount") }} ck on c.shopify_customerId = ck.shopify_customerId
left join
    {{ ref("firstOrder") }} fo
    on c.shopify_customerId = fo.shopify_customerId
left join 
{{ ref('customerSubscriptionSummary') }} cs 
 on cs.shopify_customerId = c.shopify_customerId

select
    sc.shopify_customerid,
    ck.kitCount,
    fo.firstorder_acquisitionChannel,
    fo.firstorder_discountCode,
    fo.firstorder_shippingCountry,
    ss.firstSubscriptionDate,
    ss.lastSubscriptionCancelledAt,
    ss.subscriptionsActiveCount
from {{ ref("stg_shopify__customers") }} sc
left join {{ ref("customerKitCount") }} ck using (shopify_customerid)
left join {{ ref("firstOrder") }} fo using (shopify_customerid)
left join {{ ref("customerSubscriptionSummary") }} ss using (shopify_customerid)

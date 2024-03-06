select
    sc.shopify_customerId,
    sc.recharge_latestCustomerid as recharge_customerid,
    sc.email,
    sc.createdAt,
    sc.latestSubscriptionsActiveCount as subscriptionsActiveCount,
    ss.firstSubscriptionDate,
    ss.lastSubscriptionCancelledAt
from {{ ref("inner_shopify__customer") }} sc
left join {{ ref("customerSubscriptionSummary") }} ss using (shopify_customerid)

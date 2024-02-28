select ro.*,
subscriptionsActiveCount,subscriptionsCount,firstSubscriptionDate,lastSubscriptionCancelledAt, maxsubscriptionOrderCount,
minOrderIntervalFrequency,
maxOrderIntervalFrequency

,so.validOrder 
from {{ ref('inner_recharge_order') }} ro 
left join {{ ref('recharge_orderSubscriptionSummary') }} s 
using(recharge_orderId) 
left join
    {{ ref("inner_shopify__order") }} so
    using(shopify_orderid )
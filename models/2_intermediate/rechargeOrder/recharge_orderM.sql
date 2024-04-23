with basketSize as
(select recharge_orderId , sum(lineBasketSize)  as orderBasketSize
from {{ ref('recharge_orderLineM') }}
group by recharge_orderId
)

select ro.*,b.orderBasketSize,

s.subscriptionsActiveCount,
s.subscriptionsCount,
s.firstSubscriptionDate,
s.lastSubscriptionCancelledAt, 
s.maxsubscriptionOrderCount,
s.minOrderIntervalFrequency,
s.maxOrderIntervalFrequency

,so.validOrder ,so.invalidLabel, so.customerOrderRank as shopify_customerOrderRank, so.orderCustomerType as shopify_orderCustomerType, so.shopify_customerId
from {{ ref('inner_recharge_order') }} ro 
    left join basketSize b
        using(recharge_orderId)
    left join {{ ref('recharge_orderSubscriptionSummary') }} s 
        using(recharge_orderId) 
left join
    {{ ref("shopifyOrderL") }} so
    using(shopify_orderid )

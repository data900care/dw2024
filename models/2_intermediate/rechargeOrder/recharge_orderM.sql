with basketSize as
(select recharge_orderId , sum(lineBasketSize)  as orderBasketSize
from {{ ref('recharge_orderLineM') }}
group by recharge_orderId
)

select ro.*,b.orderBasketSize,
subscriptionsActiveCount,subscriptionsCount,firstSubscriptionDate,lastSubscriptionCancelledAt, maxsubscriptionOrderCount,
minOrderIntervalFrequency,
maxOrderIntervalFrequency
,so.validOrder , so.customerOrderRank as shopify_customerOrderRank, so.orderCustomerType as shopify_orderCustomerType
from {{ ref('inner_recharge_order') }} ro 
    left join basketSize b
        using(recharge_orderId)
    left join {{ ref('recharge_orderSubscriptionSummary') }} s 
        using(recharge_orderId) 
left join
    {{ ref("shopifyOrderL") }} so
    using(shopify_orderid )
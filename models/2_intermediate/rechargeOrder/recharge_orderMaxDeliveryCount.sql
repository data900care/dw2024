with subscriptionOrderCount as 
    (select subscriptionId, max(subscriptionOrderRank) as orderCount
    from {{ ref('inner_recharge__order_line') }} o
    group by subscriptionId
)
,
orderMaxDeliveryCount  as 
    ( 
    select recharge_orderid , 
    max(s.orderCount) as maxSubscriptionOrderCount
    from {{ ref('inner_recharge__order_line') }} o
    join subscriptionOrderCount s using(subscriptionId)
    group by recharge_orderid
    )

select * from  orderMaxDeliveryCount
--where  maxSubscriptionOrderCount > 3


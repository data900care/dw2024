with
    activeCountSummary as (
        select
            recharge_orderId,
            count(*) as subscriptionsActiveCount
        from {{ ref("recharge_orderLineM") }} 
        where status = 'active'
        group by recharge_orderId
    ),
    countSummary as (
        select
            recharge_orderId,
            count(*) as subscriptionsCount
        from {{ ref("recharge_orderLineM") }} 
        group by recharge_orderId
    ),
    dateSummary as (
        select
            recharge_orderId,
            min(subscriptionCreatedAt) as firstSubscriptionDate,
            max(subscriptionCancelledAt) as lastSubscriptionCancelledAt

       from {{ ref("recharge_orderLineM") }} 
        group by recharge_orderId
    )

select recharge_orderId, ifnull(subscriptionsActiveCount,0) as subscriptionsActiveCount,
subscriptionsCount,firstSubscriptionDate,lastSubscriptionCancelledAt, m.maxsubscriptionOrderCount
from {{ ref('stg_recharge__order') }} o 
left join {{ ref('recharge_orderMaxDeliveryCount') }} m using(recharge_orderId)
left join activeCountSummary a using(recharge_orderId) 
left join countSummary c using(recharge_orderId) 
left join dateSummary d using(recharge_orderId) 

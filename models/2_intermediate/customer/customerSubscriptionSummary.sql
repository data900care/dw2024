with
    countSummary as (
        select
            c.shopify_customerId,
            sum(subscriptionsActiveCount) as subscriptionsActiveCount
        from {{ ref("inner_shopify__customer") }} c
        group by c.shopify_customerId
    ),
    dateSummary as (
        select
            c.shopify_customerid,
            min(s.createdat) as firstSubscriptionDate,
            max(s.cancelledat) as lastSubscriptionCancelledAt
        from {{ ref("inner_shopify__customer") }} c
        join {{ ref("inner_recharge__subscription") }} s using (recharge_customerId)
        where c.shopify_customerId is not null
        group by c.shopify_customerId
    )

select
    countSummary.shopify_customerId,
    subscriptionsActiveCount,
    firstSubscriptionDate,
    lastSubscriptionCancelledAt
from countSummary 
join dateSummary using (shopify_customerId)

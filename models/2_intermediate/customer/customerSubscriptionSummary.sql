with
    countSummary as (
        select
            c.shopify_customerId,
            sum(subscriptionsActiveCount) as subscriptionsActiveCount
        from {{ ref("ShopifyRechargeCustomers") }} c
        group by c.shopify_customerId
    ),
    dateSummary as (
        select
            c.shopify_customerid,
            min(s.createdat) as firstSubscriptionDate,
            max(s.cancelledat) as lastSubscriptionCancelledAt
        from {{ ref("ShopifyRechargeCustomers") }} c
        join {{ ref("stg_recharge__subscription") }} s using (recharge_customerId)
        where shopify_customerId is not null
        group by c.shopify_customerId
    )

select
    countSummary.shopify_customerId,
    subscriptionsActiveCount,
    firstSubscriptionDate,
    lastSubscriptionCancelledAt
from countSummary 
join dateSummary using (shopify_customerId)

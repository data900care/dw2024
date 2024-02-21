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
    ),
    upsellSummary as (
        select shopify_customerId, string_agg(distinct upsellCustomerType, ' & ' order by upsellCustomerType) as upsellCustomerType
        from {{ ref('upsell') }}
        join {{ ref('inner_recharge__subscription') }}
        using(subscriptionId)
        group by shopify_customerId
        
    )

select
    countSummary.shopify_customerId,
    subscriptionsActiveCount,
    firstSubscriptionDate,
    lastSubscriptionCancelledAt,
    u.upsellCustomerType 
from countSummary 
join dateSummary using (shopify_customerId)
left join upsellSummary u using(shopify_customerId)

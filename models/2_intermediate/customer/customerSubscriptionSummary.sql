with

    dateSummary as (
        select
            c.shopify_customerid,
            min(s.createdat) as firstSubscriptionDate,
            max(s.cancelledat) as lastSubscriptionCancelledAt
        from {{ ref("inner_shopify__customer") }} c
        join {{ ref("inner_recharge__subscription") }} s on s.recharge_customerId = c.recharge_latestCustomerid
        where c.shopify_customerId is not null
        group by c.shopify_customerId
    )

select
    shopify_customerId,
    
    firstSubscriptionDate,
    lastSubscriptionCancelledAt
from dateSummary 

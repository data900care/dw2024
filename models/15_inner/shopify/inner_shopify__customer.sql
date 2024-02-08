with
    latestrechargecustomerId as (
        select max(recharge_customerid) as latestid
        from {{ ref("stg_recharge__customer") }}
        group by shopify_customerid
    ),
    latestrechargecustomer as (
        select shopify_customerid, recharge_customerid, subscriptionsactivecount
        from {{ ref('stg_recharge__customer') }}
        where recharge_customerid in (select latestid from latestrechargecustomerId)
    )

select sc.*, rc.recharge_customerid, rc.subscriptionsactivecount
from {{ ref("stg_shopify__customer") }} sc
left join
    latestrechargecustomer rc using (shopify_customerid)

    -- there are multiple recharge customers for same shopify customer sometimes (10
    -- cases), externalCustomerIdEcommerce is not unique
    -- 11K customer with externalCustomerIdEcommerce Null but only 6 subscriptions on
    -- these clients.
    -- 500 customer in recharge with unexisting externalId s , not included yet
    -- why ? when they change email ?
    

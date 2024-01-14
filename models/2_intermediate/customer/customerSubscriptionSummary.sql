select
    c.shopify_customerid,
    min(s.createdat) as firstSubscriptionDate,
    max(s.cancelledat) as lastSubscriptionCancelledAt,
from {{ ref("ShopifyRechargeCustomers") }} c
join
    {{ ref("stg_recharge__subscription") }} s
    on s.recharge_customerid = c.recharge_customerid
group by c.shopify_customerid
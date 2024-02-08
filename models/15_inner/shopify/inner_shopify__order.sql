select
    *,
    rank() over (
        partition by shopify_customerid order by o.shopify_orderId
    ) as customerOrderRank,

    true as validorder

from {{ ref("stg_shopify__order") }} o

where
    o.shopify_orderid
    not in (select shopify_orderId from {{ ref("init_invalidShopifyOrders") }})

union all

select *, -1 as customerOrderRank, false as validorder

from {{ ref("stg_shopify__order") }} o

where
    o.shopify_orderId in (select shopify_orderid from {{ ref("init_invalidShopifyOrders") }})

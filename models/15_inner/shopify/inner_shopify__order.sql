select o.*, -1 as customerOrderRank, false as validorder,i.invalidLabel

from {{ ref("stg_shopify__order") }} o
join {{ ref("init_invalidOrCancelledShopifyOrders") }} i using(shopify_orderId)

union all

select
    o.*,
    rank() over (
        partition by shopify_customerid order by o.shopify_orderId
    ) as customerOrderRank,

    true as validorder,'' as invalidLabel

from {{ ref("stg_shopify__order") }} o

where
    o.shopify_orderid
    not in (select shopify_orderId from {{ ref("init_invalidOrCancelledShopifyOrders") }})


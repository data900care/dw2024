select shopify_orderId , cancelReason as invalidlabel from {{ ref("stg_shopify__order") }}
where cancelled is true
union all
select  shopify_orderId , invalidlabel from {{ ref("init_invalidShopifyOrders") }}
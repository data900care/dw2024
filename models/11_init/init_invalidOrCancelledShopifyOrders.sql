select  shopify_orderId from {{ ref("init_invalidShopifyOrders") }}
union all
select shopify_orderId from {{ ref("stg_shopify__order") }}
where cancelled is true
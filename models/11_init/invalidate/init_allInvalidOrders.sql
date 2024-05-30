select * from {{ ref('init_900invalidShopifyOrders') }}
union all
select * from {{ ref('init_invalidCanceledShopifyOrders') }}
    where shopify_orderId not in 
        (select shopify_orderId  from {{ ref('init_900invalidShopifyOrders') }})




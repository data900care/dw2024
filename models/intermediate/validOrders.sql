select * ,
rank() over (partition by shopify_customerId order by shopify_orderId ) as customerOrderRank


from {{ ref('unified_orders') }}
where shopify_orderId not in 
(
    select shopify_orderId from {{ ref('invalidOrders') }}
)
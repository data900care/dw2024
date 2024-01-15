(
select shopify_orderId, concat('Canceled: ' , cancelReason) as invalidLabel
from {{ ref('stg_shopify__orders') }} 
where cancelledAt is not null
)
union all
(
select shopify_orderId, invalidLabel
    from {{ ref('stg_shopify__orders') }} 
    join {{ ref('stg_invalidOrder_customerIds') }} i
 on  shopify_customerId = i.customerId  
)
union all
(
select shopify_orderId ,invalidLabel
    from {{ ref('stg_shopify__orderDiscount') }} od
    join {{ ref('stg_invalidOrder_testDiscountCodes') }} i
    on od.discountcode = i.discountCode and matchType = 'equal'
)
union all
(
select shopify_orderId ,invalidLabel
    from {{ ref('stg_shopify__orderDiscount') }} od
    join {{ ref('stg_invalidOrder_testDiscountCodes') }} i
    on od.discountcode like concat(i.discountCode,'%') and matchType = 'starts'
)
union all
(
select shopify_orderId ,invalidLabel
    from {{ ref('stg_shopify__orders') }} so
    join {{ ref('stg_invalidOrder_orderNames') }} i
    on so.orderName = i.orderName 
)
union all
(
select shopify_orderId ,'Initial order before upsell' as invalidLabel
    from {{ ref('stg_shopify__orderTags') }} 
    where lower(order_tag) in ('upsellcancel','upsellcncel','upsellcancell','upcellcancel','cancelupsell')
)

(
select shopify_orderId, concat('Canceled: ' , cancel_reason) as invalidLabel
from {{ ref('stg_shopify__orders') }} 
where cancelled_at is not null
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
    from {{ ref('stg_shopify__orderDiscountCodes') }} 
    join {{ ref('stg_invalidOrder_testDiscountCodes') }} i
    on orderDiscountcode = i.discountCode 
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
    from {{ ref('stg_shopify__order_tags') }} 
    where lower(order_tag) in ('upsellcancel','upsellcncel','upsellcancell','upcellcancel','cancelupsell')
)

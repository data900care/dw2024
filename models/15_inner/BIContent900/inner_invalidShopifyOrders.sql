(
select shopify_orderId, concat('Canceled: ' , cancelReason) as invalidLabel
from {{ ref('stg_shopify__order') }} 
where cancelledAt is not null
)
union all
(
select shopify_orderId, invalidLabel
    from {{ ref('stg_shopify__order') }} 
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
    from {{ ref('stg_shopify__orderDiscount') }} od
    join {{ ref('stg_invalidOrder_testDiscountCodes') }} i
    on lower(od.discountcode) like concat('%',lower(i.discountCode),'%') and matchType = 'includes' --CONTAINS_SUBSTR ?
)
union all
(
select shopify_orderId ,invalidLabel
    from {{ ref('stg_shopify__order') }} so
    join {{ ref('stg_invalidOrder_orderNames') }} i
    on so.orderName = i.orderName 
)
union all
(
select shopify_orderId ,'Initial order before upsell' as invalidLabel
    from {{ ref('stg_shopify__orderTags') }} 
    where lower(tag) in ('upsellcancel','upsellcncel','upsellcancell','upcellcancel','cancelupsell')
)
union all
(
    select shopify_orderId,  'No Recharge Order Tags'
    from {{ ref('stg_shopify__order') }} so    
    where not exists 
        (select  1 from  {{ ref('stg_shopify__orderTags') }} t
        where regexp_contains(tag,'Subscription|OTP') and t.shopify_orderId = so.shopify_orderId)
)
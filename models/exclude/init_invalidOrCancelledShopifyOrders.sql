with upsellCancellation as 
    (select shopify_orderId ,  concat('upsell:' , upsellType) as invalidLabel 
    from {{ ref("stg_shopify__order")}} 
    join {{ ref('stg_airtable_upsell') }} on  orderName = originalOrderName
    where cancelled is true 
    ),

otherCancellation as 
    (select shopify_orderId , concat('cancel: ' , cancelReason) as invalidLabel 
    from {{ ref("stg_shopify__order") }}
    where cancelled is true 
    and shopify_orderId not in (select shopify_orderId from upsellCancellation)
    and shopify_orderId not in (select shopify_orderId from {{ ref('inner_shopify_refund_transaction') }})),

fullList as 
(
select shopify_orderId, invalidLabel from  upsellCancellation
union all
select shopify_orderId, invalidLabel from  otherCancellation
union all
select  shopify_orderId , invalidLabel from {{ ref("init_invalidShopifyOrders") }}
)

select shopify_orderId , max(invalidLabel) as invalidLabel
from fullList
group by shopify_orderId
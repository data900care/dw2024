with upsellCancellation as 
    (select shopify_orderId ,  concat('upsell:' , upsellType) as invalidlabel 
    from {{ ref("stg_shopify__order")}} 
    join {{ ref('stg_airtable_upsell') }} on  orderName = originalOrderName
    where cancelled is true 
    ),

otherCancellation as 
    (select shopify_orderId , concat('cancel: ' , cancelReason) as invalidlabel 
    from {{ ref("stg_shopify__order") }}
    where cancelled is true 
    and shopify_orderId not in (select shopify_orderId from upsellCancellation))

select shopify_orderId, invalidlabel from  upsellCancellation
union all
select shopify_orderId, invalidlabel from  otherCancellation
union all
select  shopify_orderId , invalidlabel from {{ ref("init_invalidShopifyOrders") }}
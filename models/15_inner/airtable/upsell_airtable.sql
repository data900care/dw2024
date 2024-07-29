with dedupAirtable as 
(
select  min(upsellId) as minUpsellId 
from {{ ref("stg_airtable_upsell") }}
group by subscriptionId
)
select dateUpsell,
        upsellType,
        contactChannel,
        subscriptionId,
        oso.shopify_orderId as original_shopify_orderId,
        nso.shopify_orderId as new_shopify_orderId,
        nso.newOrderCustomerType
 from {{ ref("stg_airtable_upsell") }}    u
    left join {{ ref('shopifyOrderL') }} nso  on nso.orderName = u.newOrderName
    left join {{ ref('shopifyOrderL') }} oso  on oso.orderName = u.originalOrderName
where upsellId in (select minUpsellId from dedupAirtable )


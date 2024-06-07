with dedupAirtable as 
(
select  min(upsellId) as minUpsellId 
from {{ ref("stg_airtable_upsell") }}
group by subscriptionId
),
web0euroRechargeUpsell as 
(
select createdAt as dateUpsell, 'WebUpsell' as upsellType, 'web' as contactChannel,0 as subscriptionId,shopify_orderId,validorder
 from {{ ref('inner_shopify__order_line') }} 
 join {{ ref('inner_shopify__order') }} using(shopify_orderId)
 where productType = 'Recharge' and price = 0
 and shopify_orderId not in (select shopify_orderId from {{ ref('orderBundles_from_order_note_attribute') }})
)

select dateUpsell,
        upsellType,
        contactChannel,
        subscriptionId
 from {{ ref("stg_airtable_upsell") }}       
where upsellId in (select minUpsellId from dedupAirtable )
union all
select dateUpsell,
        upsellType,
        contactChannel,
        subscriptionId
from web0euroRechargeUpsell
where validOrder is true

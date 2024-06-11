with web0euroRechargeUpsell_WithoutManualUpsell as 
(
select so.createdAt as dateUpsell, 'WebUpsell' as upsellType, 'web' as contactChannel,rol.subscriptionId,so.validorder,so.shopify_orderId, sol.lineIndex, so.newOrderCustomerType
 from {{ ref('inner_shopify__order_line') }} sol
 join {{ ref('shopifyOrderL') }} so using(shopify_orderId)
 left join {{ ref('stg_recharge__order_airbyte') }} ro using(shopify_orderId)
 left join {{ ref('stg_recharge__order_line_item_airbyte') }} rol on ro.recharge_orderId = rol.recharge_orderId and rol.lineIndex = sol.lineIndex
 where productType = 'Recharge' and price = 0
 and so.shopify_orderId not in (select shopify_orderId from {{ ref('orderBundles_from_order_note_attribute') }})
 and validOrder is true
 and  not exists (select 1 from {{ ref("upsell_airtable") }} where new_shopify_orderId = sol.shopify_orderId)
),
 web0euroRechargeUpsell_PlusManualUpsell as 
(
select so.createdAt as dateUpsell, 'WebUpsell+' as upsellType, 'web' as contactChannel,rol.subscriptionId,so.validorder,so.shopify_orderId, sol.lineIndex, so.newOrderCustomerType
 from {{ ref('inner_shopify__order_line') }} sol
 join {{ ref('shopifyOrderL') }} so using(shopify_orderId)
 join {{ ref("upsell_airtable") }} ua on new_shopify_orderId = so.shopify_orderId
 left join {{ ref('stg_recharge__order_airbyte') }} ro on ro.shopify_orderId = ua.original_shopify_orderId
 left join {{ ref('stg_recharge__order_line_item_airbyte') }} rol on ro.recharge_orderId = rol.recharge_orderId and rol.sku = sol.sku
 where productType = 'Recharge' and price = 0
 and so.shopify_orderId not in (select shopify_orderId from {{ ref('orderBundles_from_order_note_attribute') }})
 and validOrder is true
)

select dateUpsell,
        upsellType,
        contactChannel,
        subscriptionId,
        new_shopify_orderId,
        newOrderCustomerType
 from {{ ref("upsell_airtable") }}   

union all

select dateUpsell,
        upsellType,
        contactChannel,
        subscriptionId,
        shopify_orderId ,
        newOrderCustomerType
from web0euroRechargeUpsell_WithoutManualUpsell

union all

select dateUpsell,
        upsellType,
        contactChannel,
        subscriptionId,
        shopify_orderId ,
        newOrderCustomerType
from web0euroRechargeUpsell_PlusManualUpsell


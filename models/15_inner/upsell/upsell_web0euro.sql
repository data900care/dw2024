with
dailySKUprice as
( select sku, createdAt, max(price) as dailyMaxPrice
from {{ ref('stg_shopify__order_line') }} sol
 join {{ ref('stg_shopify__order') }} so using(shopify_orderId)
 where price between 0 and 100 
 group by sku, createdAt
),

 upsell_web0euro_WithoutManualUpsell as 
(
select so.createdAt as dateUpsell, 'WebUpsell' as upsellType, 'web' as contactChannel,rol.subscriptionId,so.validorder,so.shopify_orderId, sol.lineIndex, so.newOrderCustomerType, dailyMaxPrice
 from {{ ref('inner_shopify__order_line') }} sol
 join {{ ref('shopifyOrderL') }} so using(shopify_orderId)
 left join {{ ref('stg_recharge__order_airbyte') }} ro using(shopify_orderId)
 left join {{ ref('stg_recharge__order_line_item_airbyte') }} rol on ro.recharge_orderId = rol.recharge_orderId and rol.lineIndex = sol.lineIndex
left join dailySKUprice d on d.sku = sol.sku and d.createdAt = so.createdAt
 where productType = 'Recharge' and price = 0
 and so.shopify_orderId not in (select shopify_orderId from {{ ref('orderBundles_from_order_note_attribute') }})
 and validOrder is true
 and  not exists (select 1 from {{ ref("upsell_airtable") }} where new_shopify_orderId = sol.shopify_orderId)
),
 upsell_web0euro_PlusManualUpsell as 
(
select so.createdAt as dateUpsell, 'WebUpsell+' as upsellType, 'web' as contactChannel,rol.subscriptionId,so.validorder,so.shopify_orderId, sol.lineIndex, so.newOrderCustomerType, dailyMaxPrice
 from {{ ref('inner_shopify__order_line') }} sol
 join {{ ref('shopifyOrderL') }} so using(shopify_orderId)
 join {{ ref("upsell_airtable") }} ua on new_shopify_orderId = so.shopify_orderId
 left join {{ ref('stg_recharge__order_airbyte') }} ro on ro.shopify_orderId = ua.original_shopify_orderId
 left join {{ ref('stg_recharge__order_line_item_airbyte') }} rol on ro.recharge_orderId = rol.recharge_orderId and rol.sku = sol.sku
 left join dailySKUprice d on d.sku = sol.sku and d.createdAt = so.createdAt
 where productType = 'Recharge' and price = 0
 and so.shopify_orderId not in (select shopify_orderId from {{ ref('orderBundles_from_order_note_attribute') }})
 and validOrder is true
)

select dateUpsell,
        upsellType,
        contactChannel,
        subscriptionId,
        shopify_orderId ,
        lineIndex,
        newOrderCustomerType,
        dailyMaxPrice as reduction
from upsell_web0euro_WithoutManualUpsell

union all

select dateUpsell,
        upsellType,
        contactChannel,
        subscriptionId,
        shopify_orderId ,
        lineIndex,
        newOrderCustomerType,
        dailyMaxPrice as reduction
from upsell_web0euro_PlusManualUpsell



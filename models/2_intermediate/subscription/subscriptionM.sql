with dedupUpsell as 
(
select  min(shopify_orderId) as min_shopify_orderId 
from {{ ref("upsellM") }}
group by subscriptionId
),
upsell as 
(select subscriptionId, upsellType 
from {{ ref("upsellM") }}
where shopify_orderId in (select min_shopify_orderId from dedupUpsell )
)

select s.*, u.upsellType
 from {{ ref('inner_recharge__subscription') }} s
left join upsell u 
    using(subscriptionId)
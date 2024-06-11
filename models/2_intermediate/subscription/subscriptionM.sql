with dedupUpsell as 
(
select  min(new_shopify_orderId) as min_new_shopify_orderId 
from {{ ref("upsellM") }}
group by subscriptionId
),
upsell as 
(select subscriptionId, upsellType 
from {{ ref("upsellM") }}
where new_shopify_orderId in (select min_new_shopify_orderId from dedupUpsell )
)

select s.*, u.upsellType
 from {{ ref('inner_recharge__subscription') }} s
left join upsell u 
    using(subscriptionId)
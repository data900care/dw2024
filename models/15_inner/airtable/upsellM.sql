with dedup as 
(
select  min(upsellId) as minUpsellId 
from {{ ref("stg_airtable_upsell") }}
group by subscriptionId
)

select dateUpsell,
        upsellType,
        contactChannel,
        subscriptionId
 from {{ ref("stg_airtable_upsell") }}       
where upsellId in (select minUpsellId from dedup )


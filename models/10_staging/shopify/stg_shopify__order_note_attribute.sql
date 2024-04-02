with

    source as (

        select *
        from {{ source("shopify", "order_note_attribute") }}
        where name in ('utm_campaign','utmCampaign','utm_content','utmContent','kitTypes','kitEssentiels','kitSolide','kitKids')
    )

select order_id as shopify_orderid, 
    value,
     name
from source

with

    source as (

        select order_id as shopify_orderid, value, name
        from {{ source("shopify", "order_note_attribute") }}
        where name in ('utm_campaign','utm_content','kitTypes','kitEssentiels','kitSolide','kitKids')
    )

select *
from source

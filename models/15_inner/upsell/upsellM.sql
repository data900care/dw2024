
select dateUpsell,
        upsellType,
        contactChannel,
        subscriptionId,
        new_shopify_orderId as shopify_orderId,
        newOrderCustomerType,
        null as reduction
 from {{ ref("upsell_airtable") }}   

union all

select dateUpsell,
        upsellType,
        contactChannel,
        subscriptionId,
        shopify_orderId ,
        newOrderCustomerType,
        reduction
from {{ ref('upsell_web0euro') }}

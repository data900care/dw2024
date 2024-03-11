with
    firstordersubscriptions as (
        select
            shopify_customerid,
            count(distinct sku) as distinctSkuCountRechargeType,
            sum(quantity) as totalsubscriptionRechargeType
        from {{ ref("firstOrderLineItems") }}
        where lower(producttype) = 'recharge'
        group by shopify_customerid
    )

select
    shopify_customerid,
    discountcode as firstorder_discountcode,
    shippingcountry as firstorder_shippingcountry,
    acquisitionchannel as firstorder_acquisitionchannel,
    utmContent as firstorder_utmContent,
    utmCampaign as firstOrder_utmCampaign,
    createdat as firstorder_shopifycreatedat,
    distinctskucountRechargeType as firstorder_distinctSkucountRechargeType,
    totalsubscriptionRechargeType as firstorder_totalsubscriptionRechargeType
from {{ ref("shopifyOrderM") }}  o
left join firstordersubscriptions f using (shopify_customerid)

where customerorderrank = 1

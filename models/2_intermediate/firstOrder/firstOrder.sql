with
    firstordersubscriptions as (
        select
            shopify_customerid,
            count(distinct sku) as distinctSkuCountRechargeType,
            sum(quantity) as totalsubscriptionRechargeType
        from {{ ref("firstOrderLineItems") }}
        where lower(producttype) = 'recharge'
        group by shopify_customerid
    ),
basketSums as (select
   shopify_orderId, sum(basketSum) as basketSum

from {{ ref("orderLinesM") }} 
    group by shopify_orderId

)
select
    shopify_customerid,
    discountcode as firstorder_discountcode,
    shippingcountry as firstorder_shippingcountry,
    acquisitionchannel as firstorder_acquisitionchannel,
    utmContent as firstorder_utmContent,
    utmCampaign as firstOrder_utmCampaign,
    createdat as firstorder_shopifycreatedat,
    total as firstorder_total,
    distinctskucountRechargeType as firstorder_distinctSkucountRechargeType,
    totalsubscriptionRechargeType as firstorder_totalsubscriptionRechargeType,
    b.basketSum as firstorder_basketSum
from {{ ref("shopifyOrderM") }}  o
left join firstordersubscriptions f using (shopify_customerid)
left join basketSums b using (shopify_orderId)

where customerorderrank = 1

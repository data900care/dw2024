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
    shopify_orderid,
    discountcode ,
    shippingcountry ,
    acquisitionchannel ,
    utmContent ,
    --utmCampaign ,
    createdat ,
    total ,
    distinctskucountRechargeType ,
    totalsubscriptionRechargeType,
    b.basketSum 
from {{ ref("shopifyOrderM") }}  o
left join firstordersubscriptions f using (shopify_customerid)
left join basketSums b using (shopify_orderId)
where customerorderrank = 1

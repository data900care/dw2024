with firstOrderSubscriptions as
(
select shopify_customerId, count(distinct sku) as distinctSkuCount, sum(quantity) as totalSubscription
from {{ ref('firstOrderLineItems') }}
where lower(productType) = 'recharge'
group by shopify_customerId
)


SELECT
    shopify_customerId,
    discountCode AS firstOrder_discountCode,
    shippingCountry AS firstOrder_shippingCountry,
    acquisitionChannel as firstOrder_acquisitionChannel ,
    createdAt as firstOrder_shopifyCreatedAt,
    distinctSkuCount as firstOrder_distinctSkuCount,
    totalSubscription as firstOrder_totalSubscription
  FROM
 {{ ref('validShopifyOrders') }} o
 left join firstOrderSubscriptions f 
 using(shopify_customerId)

   where
    customerOrderRank = 1

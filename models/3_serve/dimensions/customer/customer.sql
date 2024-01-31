with customerKitCounts as
(
select 
    shopify_customerId,
    count(distinct kitType) as distinctKitCount,
    count(*) as totalKitCount,
from {{ ref('customerValidKits') }}
group by shopify_customerId
)

select
    sc.shopify_customerid,
    ck.totalKitCount,
    distinctKitCount,
    fo.firstorder_acquisitionChannel,
    fo.firstorder_discountCode,
    fo.firstorder_shippingCountry,
    ss.firstSubscriptionDate,
    ss.lastSubscriptionCancelledAt,
    ss.subscriptionsActiveCount,
    firstOrder_distinctSkuCount,
    firstOrder_totalSubscription,
    co.cohort
from {{ ref("stg_shopify__customers") }} sc
left join customerKitCounts ck using (shopify_customerid)
left join {{ ref("firstOrder") }} fo using (shopify_customerid)
left join {{ ref("customerSubscriptionSummary") }} ss using (shopify_customerid)
left join {{ ref('stg_BIContent900__content900_tests_customer_cohort_groups') }} co using (email)

with customerBundleCounts as
(
select 
    shopify_customerId,
    count(distinct bundleType) as distinctBundleCount,
    count(*) as totalBundleCount,
from {{ ref('customerValidBundles') }}
group by shopify_customerId
)

select
    sc.shopify_customerid,
    createdAt,
    ck.totalBundleCount,
    distinctBundleCount,
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
left join customerBundleCounts ck using (shopify_customerid)
left join {{ ref("firstOrder") }} fo using (shopify_customerid)
left join {{ ref("customerSubscriptionSummary") }} ss using (shopify_customerid)
left join {{ ref('stg_BIContent900__content900_tests_customer_cohort_groups') }} co using (email)

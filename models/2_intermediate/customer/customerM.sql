with customerBundleCounts as
(
select 
    shopify_customerId,
    count(distinct bundleType) as distinctBundleCount,
    count(*) as totalBundleCount,
from {{ ref('customerBundles') }}
group by shopify_customerId
)


select
    sc.shopify_customerId,
    sc.recharge_latestCustomerid as recharge_customerid,
    sc.email,
    sc.createdAt,
    sc.latestSubscriptionsActiveCount as subscriptionsActiveCount,
    ss.firstSubscriptionDate,
    ss.lastSubscriptionCancelledAt,
    co.cohort
from {{ ref("inner_shopify__customer") }} sc
left join {{ ref("customerSubscriptionSummary") }} ss using (shopify_customerid)
left join customerBundleCounts ck using (shopify_customerid)
left join {{ ref('stg_BIContent900__content900_tests_customer_cohort_groups') }} co using (email)
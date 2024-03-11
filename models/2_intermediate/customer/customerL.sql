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
    sc.*,
    ck.totalBundleCount,
    distinctBundleCount,
    fo.firstorder_acquisitionChannel,
    fo.firstorder_discountCode,
    fo.firstorder_shippingCountry,
    fo.firstOrder_utmCampaign,
    fo.firstOrder_utmContent,
    firstOrder_distinctSkuCountRechargeType,
    firstOrder_totalSubscriptionRechargeType,
    co.cohort
from {{ ref('customerM') }} sc
left join customerBundleCounts ck using (shopify_customerid)
left join {{ ref("firstOrder") }} fo using (shopify_customerid)
left join {{ ref('stg_BIContent900__content900_tests_customer_cohort_groups') }} co using (email)

select
    o.*,
   -- d.discountCode,
    oac.acquisitionChannel,
    ifnull(orderwithtrialkit, false) as orderWithTrialKit,
    utmCo.data as utmContent,
    os.totalshipping as shipping,
    a.itemCount,
    a.totalitemPrice,
    a.totalitemQuantity,
    ifnull(bs.bundleCount,0) as bundleCount,
    ifnull(totalRefund,0) as totalRefund

from {{ ref("inner_shopify__order") }} o

left join {{ ref("orderShippingTotals") }} os using (shopify_orderId)
--left join {{ ref("stg_shopify__order_discount_code") }} d using (shopify_orderId)
left join {{ ref("orderAcquisitionChannel") }} oac using (shopify_orderId)
left join {{ ref("ordersWithTrialKit") }} owt using (shopify_orderId)
left join {{ ref("orderUTMContent") }} utmCo using (shopify_orderId)
left join {{ ref("orderLineAggregations") }} a using (shopify_orderId)
left join {{ ref('orderBundleSummary') }} bs using(shopify_orderId)
left join {{ ref('orderRefundTotal') }} rt using(shopify_orderId)
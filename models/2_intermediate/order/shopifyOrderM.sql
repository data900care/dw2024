select
    o.*,
    d.discountCode,
    oac.acquisitionChannel,
    ifnull(orderwithtrialkit, false) as orderWithTrialKit,
    utm.data as utmCampaign,
    os.totalshipping as shipping,
    a.itemCount,
    a.totalitemPrice,
    a.totalitemQuantity,
    ifnull(bs.bundleCount,0) as bundleCount

from {{ ref("inner_shopify__order") }} o

left join {{ ref("orderShippingTotals") }} os using (shopify_orderId)

left join {{ ref("stg_shopify__order_discount_code") }} d using (shopify_orderId)

left join {{ ref("orderAcquisitionChannel") }} oac using (shopify_orderId)

left join {{ ref("ordersWithTrialKit") }} owt using (shopify_orderId)
left join {{ ref("orderUTMCampaign") }} utm using (shopify_orderId)
left join {{ ref("orderLineAggregations") }} a using (shopify_orderId)
left join {{ ref('orderBundleSummary') }} bs using(shopify_orderId)
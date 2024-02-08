select
    o.*,
    d.discountCode,
    oac.acquisitionChannel,
    ifnull(orderwithtrialkit, false) as orderWithTrialKit,
    utm.data as utmCampaign,
    os.totalshipping as shipping,
    a.itemCount,
    a.totalitemPrice,
    a.totalitemQuantity

from {{ ref("inner_shopify__order") }} o

left join {{ ref("orderShippingTotals") }} os using (shopify_orderid)

left join {{ ref("stg_shopify__order_discount_code") }} d using (shopify_orderid)

left join {{ ref("orderAcquisitionChannel") }} oac using (shopify_orderid)

left join {{ ref("ordersWithTrialKit") }} owt using (shopify_orderid)
left join {{ ref("orderUTMCampaign") }} utm using (shopify_orderid)
left join {{ ref("orderLineAggregations") }} a using (shopify_orderid)

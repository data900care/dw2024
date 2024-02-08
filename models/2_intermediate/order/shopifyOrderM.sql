select
    o.*,
    d.discountcode,
    oac.acquisitionchannel,
    ifnull(orderwithtrialkit, false) as orderwithtrialkit,
    utm.data as utmcampaign,
        a.itemcount,
    a.totalitemprice,
    a.totalitemquantity

from {{ ref("inner_shopify__order") }} o

left join {{ ref("orderShippingTotals") }} os using (shopify_orderid)

left join {{ ref("stg_shopify__order_discount_code") }} d using (shopify_orderid)

left join {{ ref("orderAcquisitionChannel") }} oac using (shopify_orderid)

left join {{ ref("ordersWithTrialKit") }} owt using (shopify_orderid)
left join {{ ref("orderUTMCampaign") }} utm using (shopify_orderid)
left join
    {{ ref("orderLineAggregations") }} a using (shopify_orderid) 


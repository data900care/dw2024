select distinct o.shopify_orderid, d.acquisitionchannel
from {{ ref("stg_shopify__orders") }} o
join
    {{ ref("stg_shopify__orderDiscount") }} od
    on od.shopify_orderid = o.shopify_orderid
join
    {{
        ref(
            "stg_BIContent900__content900_mapping_DiscountCode_acquisitionChannel"
        )
    }} d on lower(od.discountcode)= lower(d.discountcode)

where
    o.shopify_orderid not in (
        select shopify_orderid from {{ ref("orderAcquisitionChannel_01_via_grapevine") }}
    )
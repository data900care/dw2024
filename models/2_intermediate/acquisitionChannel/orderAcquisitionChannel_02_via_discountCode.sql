select distinct o.shopify_orderId, d.acquisitionchannel
from {{ ref("stg_shopify__orders") }} o
join
    {{ ref("stg_shopify__orderDiscount") }} od using (shopify_orderId)
join
    {{
        ref(
            "stg_BIContent900__content900_mapping_DiscountCode_acquisitionChannel"
        )
    }} d on lower(od.discountCode)= lower(d.discountCode)

where
    o.shopify_orderId not in (
        select shopify_orderId from {{ ref("orderAcquisitionChannel_01_via_grapevine") }}
    )

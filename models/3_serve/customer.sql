select
    c.*,
    ck.kitcount,
    fo.firstorder_acquisitionchannel,
    fo.firstorder_discountcode,
    fo.firstorder_shippingcountry
from {{ ref("unifiedCustomers") }} c
left join
    {{ ref("customerKitCount") }} ck on c.shopify_customerid = ck.shopify_customerid
left join
    {{ ref("firstOrder") }} fo
    on c.shopify_customerid = fo.shopify_customerid

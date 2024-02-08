select o.*,
d.discountCode,
oac.acquisitionChannel,
ifnull(orderWithTrialKit, false) as orderWithTrialKit

from {{ ref('inner_shopify_order') }} o

left join {{ ref('orderShippingTotals') }} os
    using (shopify_orderId)

left join {{ ref('stg_shopify__order_discount_code') }} d
    using (shopify_orderId)

left join    {{ ref('orderAcquisitionChannel') }} oac
    using (shopify_orderId)

left join {{ ref('ordersWithTrialKit') }} owt 
    using (shopify_orderId)

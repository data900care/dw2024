with orderWithCustomerType as 
(select
    o.*,
    coalesce(c2020.customertype, c2023.customertype) as orderCustomerType,
    invalidLabel

from {{ ref("shopifyOrderM") }} o

left join {{ ref("orderCustomerType2020") }} c2020 using (shopify_orderid)
left join {{ ref("orderCustomerType20220117") }} c2023 using (shopify_orderid)
left join {{ ref("init_invalidOrCancelledShopifyOrders") }} i using (shopify_orderid)
)


select * from orderWithCustomerType 

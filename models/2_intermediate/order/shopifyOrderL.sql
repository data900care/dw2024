select o.*, coalesce(c2020.customertype, c2023.customertype) as ordercustomertype,

from {{ ref("shopifyOrderM") }} o

left join {{ ref("orderCustomerType2020") }} c2020 using (shopify_orderid)
left join {{ ref("orderCustomerType20220117") }} c2023 using (shopify_orderid)

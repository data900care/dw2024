with basketSums as 
(select
   shopify_orderId, sum(basketSum) as basketSum

from {{ ref("orderLinesM") }} 
    group by shopify_orderId

)


select o.*,b.basketSum , cl.cost as costLogistics,  
from {{ ref('shopifyOrderL') }} o 
left join basketSums b using (shopify_orderId) 
left join {{ ref('stg_BIContent900__content900_Country') }} c 
    on c.countryName = shippingCountry
left join {{ ref('costLogistics') }} cl 
    on cl.year = extract(year from o.createdAt) 
        and cl.month = extract(month from o.createdAt) 
        and cl.region = c.region
        and cl.clientType = o.orderCustomerType


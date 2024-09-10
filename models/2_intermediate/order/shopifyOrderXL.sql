with basketSums as 
(select
   shopify_orderId, sum(basketSum) as basketSum, sum(costProduction) as costProduction

from {{ ref("orderLinesM") }} 
    group by shopify_orderId

),
orders as 
(

select o.*,b.basketSum , cl.cost as costLogistics ,costProduction, 
    case when orderCustomerType = 'New' then averageDailyNewClientCost else 0 end as costMarketing
from {{ ref('shopifyOrderL') }} o 

left join basketSums b using (shopify_orderId) 
left join {{ ref('stg_BIContent900__content900_Country') }} c 
    on c.countryName = shippingCountry
left join {{ ref('costLogistics') }} cl 
    on cl.year = extract(year from o.createdAt) 
        and cl.month = extract(month from o.createdAt) 
        and cl.region = c.region
        and cl.clientType = o.orderCustomerType
left join {{ ref('dailyRegionalAveragecostMarketing') }} cm 
    on cm.region = o.region  and cm.spendDate = o.createdAt

)

select * from orders 
--where createdAt > '2024-01-01'
--and orderCustomerType = 'New'
-- where costMarketing > 0
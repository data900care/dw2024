with basketSums as 
(select
   shopify_orderId, sum(basketSum) as basketSum

from {{ ref("orderLinesM") }} 
    group by shopify_orderId

)


select o.*,b.basketSum  from {{ ref('shopifyOrderL') }} o 
left join basketSums b using (shopify_orderId) 

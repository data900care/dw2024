 select shopify_orderId,
   case when customerOrderRank = 1 then 'New' 
        when customerOrderRank > 1 then  
                case when orderWithTrialKit 
                        then 'Existing' 
                        else 'Recurring'
                 end
    end as customerType
from {{ ref('validShopifyOrders') }} so
where createdAt >= '2022-01-17'
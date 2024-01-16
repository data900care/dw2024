select  rc.*
from {{ ref('stg_shopify__customers') }} sc
left join  {{ ref('stg_recharge__customers') }} rc using (shopify_customerid)



--there are multiple recharge customers for same shopify customer sometimes (10 cases), externalCustomerIdEcommerce is not unique 
--11K customer with externalCustomerIdEcommerce Null but only 6 subscriptions on these clients.
--500 customer in recharge with unexisting externalId s , not included yet
--why ? when they change email ?


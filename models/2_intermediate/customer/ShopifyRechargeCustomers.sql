select  rc.*
from {{ ref('stg_shopify__customers') }} sc
left join  {{ ref('stg_recharge__customers') }} rc
on sc.shopify_customerId = rc.shopify_customerId

--there are multiple recharge customers for same shopify customer sometimes (10 cases), externalCustomerIdEcommerce is not unique 
--11K customer with externalCustomerIdEcommerce Null but only 6 subscriptions on these clients.
--why ? when they change email ?


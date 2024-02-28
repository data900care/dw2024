select rol.*, s.status,s.createdAt as  subscriptionCreatedAt, s.cancelledAt as subscriptionCancelledAt,sol.validorder
from {{ ref("inner_recharge__order_line") }} rol
left join {{ ref('stg_recharge__subscription') }} s 
 using(subscriptionId)
left join
    {{ ref("inner_shopify__order_line") }} sol
    on rol.shopify_orderid = sol.shopify_orderid
    and sol.lineindex = rol.lineindex + 1

select rol.*, sol.validorder
from {{ ref("inner_recharge__order_line") }} rol
left join
    {{ ref("inner_shopify__order_line") }} sol
    on rol.shopify_orderid = sol.shopify_orderid
    and sol.lineindex = rol.lineindex + 1

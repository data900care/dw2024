select rol.*, ro.shopify_orderid, s.createdat as subscriptioncreatedat
from {{ ref("stg_recharge__order_line_item") }} rol
join {{ ref("stg_recharge__order") }} ro using (recharge_orderid)
left join {{ ref("inner_recharge__subscription") }} s using (subscriptionId)

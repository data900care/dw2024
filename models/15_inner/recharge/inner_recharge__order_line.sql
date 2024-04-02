with ranked as 
(select rol.*, ro.shopify_orderid, ro.createdat as recharge_orderCreatedAt,
rank() over (
        partition by subscriptionId order by recharge_orderid
    ) as subscriptionOrderRank
from {{ ref("stg_recharge__order_line_item") }} rol
join {{ ref("stg_recharge__order") }} ro using (recharge_orderid)
)

select * from ranked 
--where recharge_orderId = 450653215

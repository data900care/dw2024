select * from {{ ref('inner_recharge_order') }} o 
left join {{ ref('recharge_orderSubscriptionSummary') }} s 
using(recharge_orderId) 
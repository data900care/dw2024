select * from {{ ref('stg_recharge__order') }} o 
left join {{ ref('recharge_orderSubscriptionSummary') }} s 
using(recharge_orderId) 
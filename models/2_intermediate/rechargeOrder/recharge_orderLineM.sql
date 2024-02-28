select rol.*, s.status,s.createdAt as  subscriptionCreatedAt, s.cancelledAt as subscriptionCancelledAt, s.orderIntervalFrequency,s.orderIntervalUnit
from {{ ref("inner_recharge__order_line") }} rol
left join {{ ref('stg_recharge__subscription') }} s 
 using(subscriptionId)



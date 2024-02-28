select rol.*, s.status,s.createdAt as  subscriptionCreatedAt, s.cancelledAt as subscriptionCancelledAt, s.orderIntervalFrequencyDay
from {{ ref("inner_recharge__order_line") }} rol
left join {{ ref('inner_recharge__subscription') }} s 
 using(subscriptionId)



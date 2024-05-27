select rol.*, s.status,s.createdAt as  subscriptionCreatedAt, s.cancelledAt as subscriptionCancelledAt, s.orderIntervalFrequencyDay,
p.basketSizeQuantity * quantity as lineBasketSize ,basketSizeQuantity
from {{ ref("inner_recharge__order_line") }} rol
    left join {{ ref('inner_recharge__subscription') }} s 
         using(subscriptionId)
    left join {{ ref('stg_BIContent900__content900_Product') }} p
         on rol.sku = p.sku

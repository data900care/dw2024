select s.*, u.upsellType
 from {{ ref('inner_recharge__subscription') }} s

left join {{ ref('upsellM') }} u 
    using(subscriptionId)
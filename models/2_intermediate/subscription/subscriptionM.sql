select s.*, u.upsellType
 from {{ ref('inner_recharge__subscription') }} s

left join {{ ref('upsell') }} u 
    using(subscriptionId)
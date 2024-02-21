select s.*, u.upsellCustomerType
 from {{ ref('inner_recharge__subscription') }} s

left join {{ ref('upsell') }} u 
    using(subscriptionId)
select c.shopify_customerId, 
 idSubscription,
        cancelledAt,
        createdAt,
        next_charge_scheduled_at as nextChargeScheduledAt,
        orderIntervalFrequency,
         orderIntervalUnit,
        price,
        sku,
        status
 from {{ ref('stg_recharge__subscription') }} s
join {{ ref('stg_recharge__customers') }} c 
on s.recharge_customerId = c.recharge_customerId

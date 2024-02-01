select c.shopify_customerId, 
 idSubscription,
        cancelledAt,
        createdAt,
        next_charge_scheduled_at as nextChargeScheduledAt,
        orderIntervalFrequency,
         orderIntervalUnit,
        price,
        sku,
        status,
rank() over (partition by shopify_customerId order by s.idSubscription ) as customerSubscriptionRank,
        countryCode,
        city,
        zip 
 from {{ ref('stg_recharge__subscription') }} s
join {{ ref('stg_recharge__customers') }} c 
on s.recharge_customerId = c.recharge_customerId
left join {{ ref('stg_recharge__address') }} a 
on a.id = s.idAdresse

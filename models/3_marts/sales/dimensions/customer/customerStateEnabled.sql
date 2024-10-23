with stateUpdateCatched as 
    (select shopify_customerId from {{ ref('stg_snapshot__customer_snapshot_check') }} 
    group by shopify_customerId 
    having count(distinct state) > 1)


SELECT shopify_customerId,
date_diff(cast(updatedAt as date),firstSubscriptionDate,day) as delay,
createdAt,firstSubscriptionDate,updatedAt
FROM {{ ref('stg_snapshot__customer_snapshot_check') }}  
join {{ ref('customerSubscriptionSummary') }} ss  using (shopify_customerId)
join stateUpdateCatched s using (shopify_customerId)
where  state = 'ENABLED'
qualify  RANK() OVER (PARTITION BY shopify_customerId ORDER BY updatedAt) = 1
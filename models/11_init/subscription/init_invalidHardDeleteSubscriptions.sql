select  subscriptionId,
       idAdresse,
       cancelledAt,
       createdAt,
       recharge_customerId,       
       orderIntervalFrequency,
       orderIntervalUnit,
       price,
       sku,
       status,
       cancellationReason
from {{ ref("stg_recharge__subscription_airbyte") }} s 
left join {{ ref('stg_webhook__recharge_subscription_deleted') }} hd using(subscriptionId) 
where hd.subscriptionId is null

union all

select  subscriptionId,
       idAdresse,
       cast(hd.deletedAt as date) as cancelledAt,
       createdAt,
       recharge_customerId,       
       orderIntervalFrequency,
       orderIntervalUnit,
       price,
       sku,
       'cancelled' as status,
       'hardDelete' as cancellationReason
from {{ ref("stg_recharge__subscription_airbyte") }} s 
join {{ ref('stg_webhook__recharge_subscription_deleted') }} hd using(subscriptionId) 

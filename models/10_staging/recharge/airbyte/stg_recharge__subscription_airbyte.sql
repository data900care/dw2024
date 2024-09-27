with 

source as (

    select * from {{ source('recharge_airbyte', 'recharge_subscriptions') }}

)

    select
        id as subscriptionId,
        address_id as idAdresse,
        cast(datetime(cancelled_at, "Europe/Paris") as date) as cancelledAt,
        cast(datetime(created_at, "Europe/Paris") as date) as createdAt,
        customer_id as recharge_customerId,
        --next_charge_scheduled_at,
        cast(order_interval_frequency as int) as orderIntervalFrequency,
        order_interval_unit as orderIntervalUnit,
        price,
        sku,
        status,
        cancellation_reason as cancellationReason
    from source
    where status  in ('active','cancelled') 

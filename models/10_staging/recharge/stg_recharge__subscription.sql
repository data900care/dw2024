with 

source as (

    select * from {{ source('recharge', 'subscription') }}

),

renamed as (

    select
        id as subscriptionId,
        address_id as idAdresse,
        cast(datetime(cancelled_at, "Europe/Paris") as date) as cancelledAt,
        cast(datetime(created_at, "Europe/Paris") as date) as createdAt,
        customer_id as recharge_customerId,
        next_charge_scheduled_at,
        order_interval_frequency as orderIntervalFrequency,
        order_interval_unit as orderIntervalUnit,
        cast(price as numeric) as price,
        sku,
        status
    from source

)

select * from renamed
where status in ('active','cancelled') and _fivetran_deleted = False 
--no One Time subscriptions ?
with 

source as (

    select * from {{ source('recharge', 'subscription') }}

),

renamed as (

    select
        id as idSubscription,
        cancelled_at as cancelledAt,
        created_at as createdAt,
        customer_id as recharge_customerId,
        next_charge_scheduled_at,
        order_interval_frequency as orderIntervalFrequency,
        order_interval_unit as orderIntervalUnit,
        price,
        sku,
        status
    from source

)

select * from renamed

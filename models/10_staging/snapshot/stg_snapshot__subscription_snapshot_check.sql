with 

source as (

    select * from {{ source('snapshot', 'subscription_snapshot_check') }}

),

renamed as (

    select
        
        id as subscriptionId,
        created_at as createdAt,
        updated_at as updatedAt,
        status,
        order_interval_unit as orderIntervalUnit,
        order_interval_frequency as orderIntervalFrequency,
        price,
        sku,
        customer_id as rechargeCustomerId,
        address_id as addressId,
        dbt_valid_from,
        dbt_valid_to


    from source

)

select * from renamed

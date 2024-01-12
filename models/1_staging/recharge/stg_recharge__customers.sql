with 

source as (

    select * from {{ source('recharge', 'customer') }}

),

renamed as (

    select id as recharge_customerId,
        subscriptions_active_count,
        cast(external_customer_id_ecommerce as int) as external_customer_id_ecommerce

    from source

)

select * from renamed
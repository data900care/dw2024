with 

source as (

    select * from {{ source('recharge_fivetran', 'customer') }}

)

    select id as recharge_customerId,
        subscriptions_active_count as subscriptionsActiveCount ,
        cast(external_customer_id_ecommerce as int) as shopify_customerId

    from source


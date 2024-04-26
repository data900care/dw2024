with 

source as (

    select * from {{ source('recharge_airbyte', 'recharge_customers') }}

)

    select id as recharge_customerId,
        number_active_subscriptions as subscriptionsActiveCount ,
        cast(JSON_VALUE(external_customer_id,'$.ecommerce')  as int) as shopify_customerId

    from source
    --where cast(JSON_VALUE(external_customer_id,'$.ecommerce')  as int) is not null

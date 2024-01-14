with 

source as (

    select * from {{ source('recharge', 'customer') }}

),

renamed as (

    select id as recharge_customerid,
        subscriptions_active_count as subscriptionsActiveCount ,
        cast(external_customer_id_ecommerce as int) as shopify_customerId

    from source

)

select * from renamed
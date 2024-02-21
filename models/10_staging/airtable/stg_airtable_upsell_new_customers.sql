with 

source as (

    select * from {{ source('airtable', 'airtable_airupsell___new_customers_tblxwOJVxgBxb3kwc') }}

),

renamed as (

    select
    
        orderid as orderId,
        date_upsell as dateUpsell,
        original_order as originalOrder,
        cast(upsell_subscription_id_recharge as int) as subscriptionId

    from source

)

select * from renamed

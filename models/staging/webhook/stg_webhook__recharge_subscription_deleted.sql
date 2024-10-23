with 

source as (

    select * from {{ source('webhook', 'recharge_subscription_deleted') }}

),

renamed as (

    select
        id as subscriptionId,
        deletedAt

    from source

)

select * from renamed

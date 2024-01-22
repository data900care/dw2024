with 

source as (

    select * from {{ source('shopify', 'refund') }}

),

renamed as (

    select
        id as refundId,   
        cast(datetime(created_at, "Europe/Paris") as date) as createdAt,
        order_id as shopify_orderId
    from source

)

select * from renamed

with 

source as (

    select * from {{ source('shopify', 'transaction') }}

),

renamed as (

    select
        order_id as  shopify_orderId,     
        cast(datetime(created_at, "Europe/Paris") as date) as createdAt,
        amount ,
        kind
    from source

)


select shopify_orderId, createdAt, amount
from renamed
where kind = 'refund'

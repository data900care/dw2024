with 

source as (

    select * from {{ source('shopify', 'transaction') }}

)

select order_id as  shopify_orderId,     
        cast(datetime(created_at, "Europe/Paris") as date) as createdAt,
        amount ,
        kind 
from source


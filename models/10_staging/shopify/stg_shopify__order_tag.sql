with 

source as (

    select * from {{ source('shopify', 'order_tag') }}

),

renamed as (

    select order_id as shopify_orderId,
    value as tag

    from source

)

select * from renamed

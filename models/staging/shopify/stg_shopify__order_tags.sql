with 

source as (

    select * from {{ source('shopify', 'order_tag') }}

),

renamed as (

    select order_id as shopify_orderId,
    value as order_tag

    from source

)

select * from renamed

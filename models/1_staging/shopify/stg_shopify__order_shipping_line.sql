with 

source as (

    select * from {{ source('shopify', 'order_shipping_line') }}

),

renamed as (

    select
        order_id as shopify_orderId,
        price
    from source

)

select * from renamed

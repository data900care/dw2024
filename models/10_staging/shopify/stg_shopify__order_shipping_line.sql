with 

source as (

    select * from {{ source('shopify', 'order_shipping_line') }}

)

    select
        order_id as shopify_orderId,
        price,
        title
    from source



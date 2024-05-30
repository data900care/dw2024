with 

source as (

    select * from {{ source('shopify', 'order_tag') }}

)

    select order_id as shopify_orderId,
    value as tag

    from source

    



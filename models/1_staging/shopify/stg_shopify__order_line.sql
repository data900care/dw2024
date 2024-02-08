with 

source as (

    select * from {{ source('shopify', 'order_line') }}

),

renamed as (

    select
        order_id as shopify_orderId,     
        price,
        quantity,
        sku,
        total_discount as totalDiscount,
        index as lineIndex
    from source
)

select * from renamed

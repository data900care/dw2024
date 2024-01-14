with 

source as (

    select * from {{ source('shopify', 'order_line') }}

),

renamed as (

    select
        order_id as shopify_orderId,     
        index,  
        pre_tax_price,
        price,
        product_id as productId,
        quantity,
        sku,
        total_discount,
        variant_id

    from source

)

select * from renamed

with 

source as (

    select * from {{ source('shopify', 'order_line') }}

),

renamed as (

    select
        order_id as shopify_orderId,     
        price,
        product_id as productId,
        quantity,
        sku,
        total_discount as totalDiscount,
        variant_id as variantId
    from source
)

select * from renamed

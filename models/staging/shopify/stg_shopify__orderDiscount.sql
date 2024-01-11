with 

source as (

    select * from {{ source('shopify', 'order_discount_code') }}

),

renamed as (

    select order_id as shopify_orderId,
    code as DiscountCode
    
    from source
    where code is not null and code <> ''

)

select * from renamed

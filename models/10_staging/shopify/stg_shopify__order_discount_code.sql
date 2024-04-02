with 

source as (

    select * from {{ source('shopify', 'order_discount_code') }}

)

    select order_id as shopify_orderId,
    code as discountCode
    
    from source
    where code is not null and code <> ''



with 

source as (

    select order_id as shopify_orderId, value
     from {{ source('shopify', 'order_note_attribute') }}

),


cleaned as (


    SELECT shopify_orderId,
        translate(replace(value,'\\',''),'"][','') as bundleType
        FROM  source 
    where name = 'kitTypes' and value <> '"[]"' --and order_id = 5873428889937

)

select * from cleaned
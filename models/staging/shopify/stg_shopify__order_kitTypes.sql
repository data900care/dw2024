with 

source as (

    select * from {{ source('shopify', 'order_note_attribute') }}

),


cleaned as (


    SELECT order_id as shopify_orderId,
        translate(replace(value,'\\',''),'"][','') as kitType
        FROM  source 
    where name = 'kitTypes' and value <> '"[]"' --and order_id = 5873428889937

),

splited as (
     SELECT * EXCEPT(kt) REPLACE(kt AS kitType)
FROM  cleaned,
UNNEST(SPLIT(kitType)) kt

)

select * from splited

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
     SELECT * EXCEPT(kt) REPLACE(ltrim(kt) AS kitType)
FROM  cleaned,
UNNEST(SPLIT(kitType)) kt

)

select * from splited
union all
SELECT order_id as shopify_orderId,'essentiels' as kitType
        FROM  source
    where name like 'kitEssentiels'  and  value = '"true"' 
union all
SELECT order_id as shopify_orderId,'solid' as kitType
        FROM  source
    where name like 'kitSolide'  and  value = '"true"' 
union all
SELECT order_id as shopify_orderId,'kids' as kitType
        FROM  source
    where name like 'kitKids'  and  value = '"true"' 
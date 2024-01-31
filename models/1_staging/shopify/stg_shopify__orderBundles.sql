with 

source as (

    select * from {{ source('shopify', 'order_note_attribute') }}

),


cleaned as (


    SELECT order_id as shopify_orderId,
        translate(replace(value,'\\',''),'"][','') as bundleType
        FROM  source 
    where name = 'kitTypes' and value <> '"[]"' --and order_id = 5873428889937

),

splited as (
     SELECT * EXCEPT(kt) REPLACE(ltrim(kt) AS bundleType)
FROM  cleaned,
UNNEST(SPLIT(bundleType)) kt

)

select * from splited
union all
SELECT order_id as shopify_orderId,'essentiels' as bundleType
        FROM  source
    where name like 'kitEssentiels'  and  value = '"true"' 
union all
SELECT order_id as shopify_orderId,'solid' as bundleType
        FROM  source
    where name like 'kitSolide'  and  value = '"true"' 
union all
SELECT order_id as shopify_orderId,'kids' as bundleType
        FROM  source
    where name like 'kitKids'  and  value = '"true"' 
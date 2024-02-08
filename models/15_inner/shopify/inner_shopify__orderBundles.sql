with 



splited as (
     SELECT * EXCEPT(kt) REPLACE(ltrim(kt) AS bundleType)
FROM  {{ ref('stg_shopify__order_note_attribute') }},
UNNEST(SPLIT(bundleType)) kt

)

select * from splited
union all
SELECT order_id as shopify_orderId,'essentials' as bundleType
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
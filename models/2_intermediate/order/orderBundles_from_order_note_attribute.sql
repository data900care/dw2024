with 
source_newKitTypes as (
select
           shopify_orderId,
        translate(replace(value,'\\',''),'"][','') as bundleType
        from {{ ref('stg_shopify__order_note_attribute') }}
        where name = 'kitTypes' and value <> '"[]"'  
),
source_oldKitTypes as (
select
           shopify_orderId,name
        from {{ ref('stg_shopify__order_note_attribute') }}
        where name in ('kitEssentiels','kitSolide','kitKids') and value = '"true"'  
),




splited as (
     SELECT * EXCEPT(kt) REPLACE(ltrim(kt) AS bundleType)
FROM  source_newKitTypes,
UNNEST(SPLIT(bundleType)) kt

)

select * from splited
union all
SELECT shopify_orderId,'essentials' as bundleType
        FROM  source_oldKitTypes
    where name like 'kitEssentiels'  
union all
SELECT shopify_orderId,'solid' as bundleType
        FROM  source_oldKitTypes
    where name like 'kitSolide'  
union all
SELECT shopify_orderId,'kids' as bundleType
        FROM  source_oldKitTypes
    where name like 'kitKids' 
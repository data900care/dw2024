with extracted as (

        select sku, 'home' as bundletype from {{ ref('stg_BIContent900__content900_Product') }}
        where kit_home 

        union all

        select sku, 'kids' as bundletype from {{ ref('stg_BIContent900__content900_Product') }}        
        where kit_kids

        union all

        select sku, 'solid' as bundletype from {{ ref('stg_BIContent900__content900_Product') }}        
        where kit_solid

        union all

        select sku, 'essentials' as bundletype from {{ ref('stg_BIContent900__content900_Product') }}        
        where kit_essentials

    )

select *
from extracted
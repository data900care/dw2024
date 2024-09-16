with

    source as (

        select * from {{ source("BIContent900", "content900_Product_Variant") }}

    ),

    renamed as (

        select sku, 'home' as bundletype

        from source
        where kit_home 
        union all
        select sku, 'kids' as bundletype
        from source
        where kit_kids
        union all
        select sku, 'solid' as bundletype
        from source
        where kit_solid 
        union all
        select sku, 'essentials' as bundletype
        from source
        where kit_essentials 

    )

select *
from renamed

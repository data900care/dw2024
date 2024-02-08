with

    source as (

        select * from {{ source("BIContent900", "content900_Product_Variant") }}

    ),

    renamed as (

        select sku, 'home' as bundletype

        from source
        where cast(kit_home as bool) = true
        union all
        select sku, 'kids' as bundletype
        from source
        where cast(kit_kids as bool) = true
        union all
        select sku, 'solid' as bundletype
        from source
        where cast(kit_solid as bool) = true
        union all
        select sku, 'essentials' as bundletype
        from source
        where cast(kit_essentials as bool) = true

    )

select *
from renamed

with 

source as (

    select * from {{ source('BIContent900', 'content900_Product') }}

),

renamed as (

    select
 
        sku,
        type,
        name,
        category,
        cast(kit_home as bool) as kitHome,
        cast(kit_kids as bool) as KitKids,
        cast(kit_solid as bool) as KitSolid,
        trial_kit as trialKit,
        product_id,
        variant_id,
        kit_essentials as kitEssentials,
        quantity_refill as quantityRefill,
        color_and_perfume ,
        weight_of_plastic as weightPlastic,
        __products_per_basket as productsPerBasket

    from source

)

select * from renamed


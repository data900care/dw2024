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
        kit_home as kitHome,
        kit_kids as KitKids,
        kit_solid as KitSolid,
        trial_kit as KitTrial,
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

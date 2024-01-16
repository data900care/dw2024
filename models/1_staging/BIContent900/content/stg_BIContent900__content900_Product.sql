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
        trial_kit as trialKit,        

        cast(kit_home as bool) as kitHome,
        cast(kit_kids as bool) as kitKids,
        cast(kit_solid as bool) as kitSolid,
        cast(kit_essentials as bool) as kitEssentials,                
        cast(quantity_refill as int) as quantityRefill,
        
        cast(weight_of_plastic as int) as  weightPlastic,
        cast(__products_per_basket as int) as productsPerBasket,
        
        cast(product_id as int) as product_id ,
        cast(variant_id as int) as variant_id,
        color_and_perfume 
    from source

)

select * from renamed


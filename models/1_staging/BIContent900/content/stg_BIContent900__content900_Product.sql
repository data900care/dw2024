with 

source as (

    select * from {{ source('BIContent900', 'content900_Product_Variant') }}

),

renamed as (

    select
 
        sku,
        type as productType,
        name,
        category as productCategory,
        trial_kit as trialKit,        
        case when lower(trial_kit) = lower("Trial Kit") then true else false end as isTrialKit,
        cast(kit_home as bool) as kitHome,
        cast(kit_kids as bool) as kitKids,
        cast(kit_solid as bool) as kitSolid,
        cast(kit_essentials as bool) as kitEssentials,                
        cast(quantity_refill as int) as quantityRefill,
        
        cast(weight_of_plastic as int) as  weightPlastic,
        cast(__products_per_basket as int) as productsPerBasket,
        
        color_and_perfume 
    from source

)

select * from renamed


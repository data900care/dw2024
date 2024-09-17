with 

source as (

    select * from {{ source('BIContent900', 'content900_Product_Variant') }}

)

    select
 
        sku,
        type as productType,
        name,
        category as productCategory,
        trial_kit as trialKit,        
        case when lower(trial_kit) = lower("Trial Kit") then true else false end as isTrialKit,
             
        cast(quantity_refill as int) as quantityRefill,
        cast(recurring_order_quantity_divide as int) as recurring_order_quantity_divide,
        
        --cast(weight_of_plastic as int) as  weightPlastic,
        cast(basket_size_quantity as int) as basketSizeQuantity,
        
        color_and_perfume ,
        cast(heroSKU  as boolean) as heroSKU,

        cast(kit_home as bool) as kit_home,
        cast(kit_kids as bool) as kit_kids,
        cast(kit_solid as bool) as kit_solid,
        cast(kit_essentials as bool) as kit_essentials
    from source




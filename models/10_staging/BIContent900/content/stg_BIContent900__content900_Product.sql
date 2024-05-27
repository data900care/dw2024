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
        cast(heroSKU  as boolean) as heroSKU
    from source




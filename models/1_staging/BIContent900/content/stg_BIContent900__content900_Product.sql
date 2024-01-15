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
        kit_home,
        kit_kids,
        kit_solid,
        trial_kit,
        product_id,
        variant_id,
        kit_essentials,
        quantity_refill,
        color_and_perfume,
        weight_of_plastic,
        __products_per_basket

    from source

)

select * from renamed

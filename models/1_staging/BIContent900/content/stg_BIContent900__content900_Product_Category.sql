with 

source as (

    select * from {{ source('BIContent900', 'content900_Product_Category') }}

),

renamed as (

    select
      
        category,
        coeff_market_vs_900_product_size,
        water_consumption_market_product,
        weight_of_plastic_market_product

    from source

)

select * from renamed

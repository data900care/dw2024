with 

source as (

    select * from {{ source('BIContent900', 'content900_Product_Category') }}

),

renamed as (

    select
      
        category,
        cast(replace(coeff_market_vs_900_product_size,',','.') as numeric) as coeffMarketSize,
        cast(replace(water_consumption_market_product,',','.') as numeric) as equivalentProductWater,
        cast(weight_of_plastic_market_product as int) as equivalentProductPlasticWeight

    from source

)

select * from renamed

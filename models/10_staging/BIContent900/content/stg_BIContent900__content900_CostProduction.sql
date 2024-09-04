with 

source as (

    select * from {{ source('BIContent900', 'content900_CostProduction') }}

),

renamed as (

    select
       
       
        year,
        month,
        productType,
        productCategory,
        cost

    from source

)

select * from renamed

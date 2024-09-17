with 

source as (

    select * from {{ source('BIContent900', 'content900_CostProduction') }}

),

renamed as (

    select
       
       
        cast(year as int) as year,
        cast(month as int) as month,
        productType,
        productCategory,
        cast(replace(cost,',','.') as decimal) as cost

    from source

)

select * from renamed

with 

source as (

    select * from {{ source('BIContent900', 'content900_Product_Category') }}

)



    select
      
        category,
        cast(display_order as int) as display_order


    from source



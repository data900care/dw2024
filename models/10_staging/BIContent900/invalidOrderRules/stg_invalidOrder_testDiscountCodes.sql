with 

source as (

    select * from {{ source('BIContent900', 'content900_invalidOrder_testDiscountCodes') }}

)
    select 
    
    *

    from source


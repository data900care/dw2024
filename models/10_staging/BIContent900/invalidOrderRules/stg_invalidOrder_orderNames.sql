with 

source as (

    select * from {{ source('BIContent900', 'content900_invalidOrder_orderNames') }}

)

    select 
    
    *

    from source


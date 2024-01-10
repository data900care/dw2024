with 

source as (

    select * from {{ source('BIContent900', 'content900_invalidOrder_orderNames') }}

),

renamed as (

    select 
    
    *

    from source

)

select * from renamed
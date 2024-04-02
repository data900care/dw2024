with 

source as (

    select * from {{ source('BIContent900', 'content900_invalidOrder_customerIds') }}

)
    select 
    cast(customerId as int) as customerId,
    invalidLabel

    from source


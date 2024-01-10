with 

source as (

    select * from {{ source('BIContent900', 'content900_invalidOrder_customerIds') }}

),

renamed as (
    select 
    cast(customerId as int) as customerId,
    invalidLabel

    from source

)

select * from renamed
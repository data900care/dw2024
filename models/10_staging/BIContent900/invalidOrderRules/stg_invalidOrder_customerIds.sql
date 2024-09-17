with 

source as (

    select * from {{ source('BIContent900', 'content900_invalidOrder_customerIds') }}

)
    select 
    cast(customer_id as int) as customerId,
    invalid_label as invalidLabel

    from source


with 

source as (

    select * from {{ source('BIContent900', 'content900_invalidOrder_orderNames') }}

)

    select 
    
    order_name as orderName,
    invalid_label as invalidLabel

    from source


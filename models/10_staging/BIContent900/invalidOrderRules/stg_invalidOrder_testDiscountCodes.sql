with 

source as (

    select 
    matchType, 
    rtrim(DiscountCode) as DiscountCode, 
    invalidLabel
    from {{ source('BIContent900', 'content900_invalidOrder_testDiscountCodes') }}

)
    select  *
    from source


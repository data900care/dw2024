with 

source as (

    select 
    match_type as matchType, 
    rtrim(discount_code) as DiscountCode, 
    invalid_label as invalidLabel
    from {{ source('BIContent900', 'content900_invalidOrder_testDiscountCodes') }}

)
    select  *
    from source


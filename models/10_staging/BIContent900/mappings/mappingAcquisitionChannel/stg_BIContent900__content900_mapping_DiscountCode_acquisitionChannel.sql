with 

source as (

    select * from {{ source('BIContent900', 'content900_mapping_orderDiscountCode_acquisitionChannel') }}

)

    select
        code as discountCode,
        acquisitionchannel as acquisitionChannel

    from source



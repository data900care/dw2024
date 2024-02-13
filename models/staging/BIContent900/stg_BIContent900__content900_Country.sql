with 

source as (

    select * from {{ source('BIContent900', 'content900_Country') }}

),

renamed as (

    select
        code as countryCode,
        name as countryName,
        `group` as countryGroup,
        `order` as countryOrder

    from source

)

select * from renamed

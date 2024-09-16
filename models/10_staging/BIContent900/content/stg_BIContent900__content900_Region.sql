with 

source as (

    select * from {{ source('BIContent900', 'content900_Region') }}

),

renamed as (

    select Name as region

    from source

)

select * from renamed

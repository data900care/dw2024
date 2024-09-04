with 

source as (

    select * from {{ source('BIContent900', 'content900_Region') }}

),

renamed as (

    select Name

    from source

)

select * from renamed

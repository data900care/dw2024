with 

source as (

    select * from {{ source('shopify', 'customer') }}

),

renamed as (

    select id

    from source

)

select * from renamed

with 

source as (

    select * from {{ source('shopify', 'customer') }}

),

renamed as (

    select id as shopify_customerId,
    email

    from source

)

select * from renamed

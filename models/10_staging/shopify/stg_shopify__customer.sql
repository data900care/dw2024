with 

source as (

    select * from {{ source('shopify', 'customer') }}

)

    select id as shopify_customerId,
    email,
    cast(datetime(created_at, "Europe/Paris") as date) as createdAt

    from source



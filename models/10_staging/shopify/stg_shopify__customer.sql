with 

source as (

    select * from {{ source('shopify', 'customer') }}

)

    select id as shopify_customerId,
    email,
    cast(datetime(created_at, "Europe/Paris") as date) as createdAt,
    total_spent as totalSpent,
    upper(state) as state

    from source



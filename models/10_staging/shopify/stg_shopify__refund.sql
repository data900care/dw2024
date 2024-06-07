with 

source as (

    select * from {{ source('shopify', 'refund') }}

),

renamed as (

    select
        id,
        created_at,
        order_id,
        processed_at
    from source

)

select * from renamed

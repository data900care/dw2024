with 

source as (

    select * from {{ source('shopify', 'customer_address') }}

),

renamed as (

    select
        customer_id,
        address_1,
        address_2,
        city,
        country,
        first_name,
        is_default,
        zip

    from source

)

select * from renamed
where is_default is true

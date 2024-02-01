with 

source as (

    select * from {{ source('recharge', 'address') }}

),

renamed as (

    select
        id,
        city,
        country_code as countryCode,
        province,
        zip

    from source

)

select * from renamed

with 

source as (

    select * from {{ source('recharge', 'address') }}

)

    select
        id,
        city,
        country_code as countryCode,
        province,
        zip

    from source


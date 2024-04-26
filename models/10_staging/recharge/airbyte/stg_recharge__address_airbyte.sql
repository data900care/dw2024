with 

source as (

    select * from {{ source('recharge_airbyte', 'recharge_addresses') }}

)

    select
        id,
        city,
        country_code as countryCode,
        province,
        zip

    from source


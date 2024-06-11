with 

source as (

    select * from {{ source('recharge_fivetran', 'address') }}

)

    select
        id,
        city,
        country_code as countryCode,
        province,
        zip

    from source


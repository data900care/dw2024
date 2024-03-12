with 

source as (

    select * from {{ source('externalDataAirbyte', 'ga4_demographic_country_report') }}

),

renamed as (

    select
        date,
        country,
        totalusers

    from source

)

select * from renamed

with 

source as (

    select * from {{ source('externalDataAirbyte', 'ga4_demographic_country_report') }}

),

renamed as (

    select
        PARSE_DATE('%Y%m%d', date) as date,
        country,
        totalusers

    from source

)

select * from renamed

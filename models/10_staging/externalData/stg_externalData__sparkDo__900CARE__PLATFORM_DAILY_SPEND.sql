with 

source as (

    select * from {{ source('externalDataAirbyte', 'sparkDo__900CARE__PLATFORM_DAILY_SPEND') }}

),

renamed as (

    select
        campaign_channel as campaignChannel,
        date+1 as spendDate, -- Airbyte imports has a bug, changes the original date to date-1 !! 
        cost,
        campaign_location as campaignLocation

    from source

)

select * from renamed

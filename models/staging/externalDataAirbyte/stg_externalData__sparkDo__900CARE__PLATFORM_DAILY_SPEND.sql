with 

source as (

    select * from {{ source('externalDataAirbyte', 'sparkDo__900CARE__PLATFORM_DAILY_SPEND') }}

),

renamed as (

    select
        campaign_channel as campaignChannel,
        date as spendDate,
        cost

    from source

)

select * from renamed

with 

source as (

    select * from {{ source('BIContent900', 'content900_Country') }}

),

renamed as (

    select
        /*900 Definitions*/
        name as countryName,
        `group` as countryGroup,
        `order` as countryOrder,

        /*Mapping Columns*/
        Spark_campaign_location as campaignLocation,
        GoogleAnalytics_Name as googleAnalyticsName,
        yotpo_ReviewerCountryCode as yotpoReviewerCountryCode,
        Retently_Country as retentlyCountry,
        Recharge_AddressCountryCode as recharge_countryCode

    from source

)

select * from renamed

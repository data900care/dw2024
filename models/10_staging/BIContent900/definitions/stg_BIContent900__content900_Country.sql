with 

source as (

    select * from {{ source('BIContent900', 'content900_Country') }}

)

    select
        /*900 Definitions*/
        name as countryName,
        region as region,
        `order` as countryOrder,

        /*Mapping Columns*/

        GoogleAnalytics_Name as googleAnalyticsName,
        yotpo_ReviewerCountryCode as yotpoReviewerCountryCode,
        Retently_Country as retentlyCountry,
        Recharge_AddressCountryCode as recharge_countryCode

    from source



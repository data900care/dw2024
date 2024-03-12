select 
        countryCode,
        countryName,
        countryGroup,
        countryOrder,
        campaignLocation
        
 from {{ ref('stg_BIContent900__content900_Country') }}
select 
        countryName,
        region,
        countryOrder
        
 from {{ ref('stg_BIContent900__content900_Country') }}
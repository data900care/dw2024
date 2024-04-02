select 
createdAt,
score, 
countryName,
customerType,
customerStatus
from {{ ref('stg_retently__retently_feedback') }} f
       left join {{ ref('stg_BIContent900__content900_Country') }} c
            on   c.RetentlyCountry = f.country

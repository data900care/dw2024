select ga.date , ga.totalUsers, c.countryName, ga.conversions , ga.country
from {{ ref('stg_externalData__ga4_demographic_country_report') }} ga
left join {{ ref('stg_BIContent900__content900_Country') }} c 
on c.GoogleAnalyticsName = ga.country
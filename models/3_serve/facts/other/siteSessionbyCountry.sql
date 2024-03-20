select 
date , totalUsers, countryName, conversions
 from {{ ref('siteSessionbyCountryM') }}
where countryName is not null
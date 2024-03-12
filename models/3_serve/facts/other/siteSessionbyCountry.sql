select * from {{ ref('siteSessionbyCountryM') }}
where countryName is not null
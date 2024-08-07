SELECT originalOrderName 
FROM {{ ref('stg_airtable_upsell') }}
where originalOrderName is not null
group by originalOrderName
having count(*) > 1
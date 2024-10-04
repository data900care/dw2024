
 with
filtered_events as 
(select geo_country, ga_session_id, 
customer_id, user_id,
REPLACE(replace(event_name,'-','_'), ' ','_') as event_name, event_timestamp
FROM  {{ ref('stg_ga4_events') }}
where event_name in
 ('session_start','GA4 - product_page_cta','add_to_cart','funnel_step_antichurn_cta','add_upsell_to_cart','begin_checkout','purchase')
 or
 event_name in ('page_view','view_item') and CONTAINS_SUBSTR(page_location,'product')
),
session_customer_id as 
(
  select distinct  ga_session_id ,customer_id ,user_id,
  from filtered_events where ga_session_id is not null and (customer_id is not null or user_id is not null)
),

session_first_geo_country as 
(select distinct ga_session_id, first_value(geo_country) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as geo_country
FROM filtered_events
where ga_session_id is not null and geo_country is not null),

 pivoted As 
(SELECT * FROM (
  SELECT  ga_session_id, event_name,
         MIN(event_timestamp) OVER (PARTITION BY ga_session_id, event_name) firstTimeStamp
    FROM filtered_events, UNNEST([event_timestamp]) date0
    where   ga_session_id is not null
)  
PIVOT (ANY_VALUE(firstTimeStamp) first 
FOR event_name IN ('session_start','GA4___product_page_cta','add_to_cart','funnel_step_antichurn_cta','add_upsell_to_cart','begin_checkout','purchase','page_view','view_item'))
)


select  s.customer_id,c.geo_country,s.user_id, 
p.*  , u.utm_Source, u.utm_Content, u.utm_Campaign_Name
from pivoted p
left join session_customer_id s using(ga_session_id)
left join session_first_geo_country c using(ga_session_id)
left join {{ ref('session_first_UTM') }} u using(ga_session_id)
where first_session_start is not null-- and user_id is not null
with customer_first_sessionId as
(SELECT customer_id, first_value(ga_session_id) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC ) as ga_session_id
FROM {{ ref('stg_ga4_events') }}
where customer_id is not null and ga_session_id is not null)
,
session_first_google_data as 
(
   select distinct ga_session_id, 
   first_value(device_category) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as device_category,
   first_value(device_mobile_brand_name) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as device_mobile_brand_name,
   first_value(geo_country) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as geo_country,
   first_value(traffic_source_source) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as traffic_source_source
   FROM {{ ref('stg_ga4_events') }}
   where ga_session_id is not null 
        and (device_category is not null or geo_country is not null or traffic_source_source is not null)
)




SELECT distinct customer_id
,ga_session_id
,device_category
,device_mobile_brand_name
,geo_country
,traffic_source_source
,utm_Source
,utm_Content
,utm_Campaign_Name
FROM customer_first_sessionId
left join session_first_google_data using(ga_session_id)
left join {{ ref('session_first_UTM') }} using(ga_session_id)

--where manual_source is not null
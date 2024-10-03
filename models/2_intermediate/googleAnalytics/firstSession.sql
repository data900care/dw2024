with customer_first_sessionId as
(SELECT customer_id, first_value(ga_session_id) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC ) as ga_session_id
FROM {{ ref('stg_ga4_events') }}
where customer_id is not null and ga_session_id is not null)
,
session_first_device_category as 
(select ga_session_id, first_value(device_category) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as device_category
FROM {{ ref('stg_ga4_events') }}
where customer_id is not null and ga_session_id is not null and device_category is not null)
,
session_first_device_mobile_brand_name as 
(select ga_session_id, first_value(device_mobile_brand_name) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as device_mobile_brand_name
FROM {{ ref('stg_ga4_events') }}
where customer_id is not null and ga_session_id is not null and device_mobile_brand_name is not null)
,
session_first_geo_country as 
(select ga_session_id, first_value(geo_country) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as geo_country
FROM {{ ref('stg_ga4_events') }}
where customer_id is not null and ga_session_id is not null and geo_country is not null)
,
session_first_traffic_source_source as 
(select ga_session_id, first_value(traffic_source_source) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as traffic_source_source
FROM {{ ref('stg_ga4_events') }}
where customer_id is not null and ga_session_id is not null and traffic_source_source is not null)
,
session_first_manual_source as 
(select ga_session_id, first_value(manual_source) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as manual_source
FROM {{ ref('stg_ga4_events') }}
where customer_id is not null and ga_session_id is not null and manual_source is not null)
,
session_first_manual_content as 
(select ga_session_id, first_value(manual_source) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as manual_content
FROM {{ ref('stg_ga4_events') }}
where customer_id is not null and ga_session_id is not null and manual_content is not null)
,
session_first_manual_campaign_name as 
(select ga_session_id, first_value(manual_campaign_name) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as manual_campaign_name
FROM {{ ref('stg_ga4_events') }}
where customer_id is not null and ga_session_id is not null and manual_campaign_name is not null)

SELECT  customer_id,ga_session_id
,device_category
,device_mobile_brand_name
,geo_country
,traffic_source_source
,manual_source as UTM_source
,manual_content as UTM_content
,manual_campaign_name as UTM_campaign_name
FROM customer_first_sessionId
left join session_first_device_category using(ga_session_id)
left join session_first_device_mobile_brand_name using(ga_session_id)
left join session_first_geo_country using(ga_session_id)
left join session_first_traffic_source_source using(ga_session_id)
left join session_first_manual_source using(ga_session_id)
left join session_first_manual_content using(ga_session_id)
left join session_first_manual_campaign_name using(ga_session_id)


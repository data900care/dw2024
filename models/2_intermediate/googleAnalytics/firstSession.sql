with firstSessionId as
(SELECT customer_id, first_value(ga_session_id) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC ) as ga_session_id
FROM {{ ref('stg_ga4_events') }}
where customer_id is not null and ga_session_id is not null)

SELECT distinct customer_id,ga_session_id,device_category,device_mobile_brand_name,geo_country,traffic_source_source
,manual_source as UTM_source
,manual_content as UTM_content
,manual_campaign_name as UTM_campaign_name
FROM {{ ref('stg_ga4_events') }}
join firstSessionId using(customer_id,ga_session_id)
where customer_id is not null and ga_session_id is not null
--order by user_id
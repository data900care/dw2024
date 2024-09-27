with firstSessionId as
(SELECT user_id, first_value(ga_session_id) OVER (PARTITION BY user_id ORDER BY event_timestamp ASC ) as ga_session_id
FROM {{ ref('stg_ga4_events') }}
where user_id is not null and ga_session_id is not null)

SELECT distinct user_id,ga_session_id,device_category,device_mobile_brand_name,geo_country,traffic_source_source
,manual_source as UTM_source
,manual_content as UTM_content
,manual_campaign_name as UTM_campaign_name
,geo_country
FROM {{ ref('stg_ga4_events') }}
join firstSessionId using(user_id,ga_session_id)
where user_id is not null and ga_session_id is not null
--order by user_id
select ga_session_id, 
first_value(manual_source) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as utm_Source,
first_value(manual_content) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as utm_Content,
first_value(manual_campaign_name) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC ) as utm_Campaign_Name
FROM {{ ref('stg_ga4_events') }}
where  ga_session_id is not null and 
    (manual_source is not null or manual_content is not null or manual_campaign_name is not null)

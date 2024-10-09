select 
event_date
,event_timestamp
,event_name

,ga_session_id

,customer_id
,user_id

,page_location
,url_parameters
,page_referrer

,device_category
,device_mobile_brand_name

,geo_country 

,traffic_source_source

,manual_source as utm_source
,manual_content as utm_content
,manual_campaign_name as utm_campaign_name
,manual_medium  as utm_medium

,m.new_page_location
     from {{ ref('stg_ga4_events') }} s
left join {{ ref('stg_BIContent900__content900_mapping_ga4_page_location') }} m
    using (page_location)
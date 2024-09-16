select 
event_date
,event_timestamp
,event_name

,ga_session_id
,user_id 
,user_pseudo_id
,batch_page_id

,page_location
,url_parameters
,page_referrer

,device_category
,device_mobile_brand_name

,geo_country 

,traffic_source_source

,manual_source as UTM_source
,manual_content as UTM_content
,manual_campaign_name as UTM_campaign_name

,m.new_page_location
     from {{ ref('stg_ga4_events') }} s
left join {{ ref('stg_BIContent900__content900_mapping_ga4_page_location') }} m
    using (page_location)
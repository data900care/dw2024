
with tmp_unnest as (
select
parse_date('%Y%m%d', event_date) as event_date
,timestamp_micros(event_timestamp)as event_timestamp
,event_name
,user_pseudo_id
,user_id
,(select value.string_value from unnest(event_params) where key = 'page_location') as page_location
,(select value.string_value from unnest(event_params) where key = 'page_referrer') as page_referrer
,(select value.string_value from unnest(event_params) where key = 'link_url') as link_url
,(select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id
,(select value.int_value from unnest(event_params) where key = 'ga_session_number') as ga_session_number
,(select value.int_value from unnest(event_params) where key = 'batch_page_id') as batch_page_id
,device.category as device_category
,device.mobile_brand_name as device_mobile_brand_name
,device.web_info.browser as device_browser
,device.language as device_language
,geo.country as geo_country
,traffic_source.name as traffic_source_name
,traffic_source.medium as traffic_source_medium
,traffic_source.source as traffic_source_source
from {{ source('ga4_raw', 'full_events') }}

)

select
event_date
,event_timestamp
,event_name

,ga_session_id
,user_id 
,user_pseudo_id
,batch_page_id

,regexp_extract(page_location, r'^[^?]*')  as page_location
,split(page_location, '?')[SAFE_OFFSET(1)] as url_parameters
,page_referrer

,device_category
,device_mobile_brand_name

,geo_country 

,traffic_source_source

from tmp_unnest
--where event_name in ('page_view','first_visit')

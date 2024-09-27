{{ config(
    materialized="incremental",
    cluster_by = "event_name",
    incremental_strategy="insert_overwrite",
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day",
      "copy_partitions": true
    }
) }}

--if schema change RUN the following
--dbt run --full-refresh --models stg_ga4_events

with tmp_unnest as (
select
parse_date('%Y%m%d', event_date) as event_date
,timestamp_micros(event_timestamp)as event_timestamp
,event_name
--,user_pseudo_id
,user_id
-- EVENT PARAMS UNNESTED
,(select value.string_value from unnest(event_params) where key = 'page_location') as page_location
,(select value.string_value from unnest(event_params) where key = 'page_referrer') as page_referrer
,(select value.string_value from unnest(event_params) where key = 'link_url') as link_url
,(select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id
,(select value.int_value from unnest(event_params) where key = 'ga_session_number') as ga_session_number
,(select value.int_value from unnest(event_params) where key = 'batch_page_id') as batch_page_id
-- USER PROPERTIES  UNNESTED
,(select value.int_value from unnest(user_properties) where key = 'customer_id') as user_properties_customer_id
,(select value.string_value from unnest(user_properties) where key = 'user_id') as user_properties_user_id
,device.category as device_category
,device.mobile_brand_name as device_mobile_brand_name
,device.web_info.browser as device_browser
,device.language as device_language
,geo.country as geo_country
,traffic_source.name as traffic_source_name
,traffic_source.medium as traffic_source_medium
,traffic_source.source as traffic_source_source


,collected_traffic_source.manual_source
,collected_traffic_source.manual_content
,collected_traffic_source.manual_campaign_name
from {{ source('ga4_raw', 'full_events') }}

)

select
event_date
,event_timestamp
,event_name

,ga_session_id
,user_id 
,user_pseudo_id
,user_properties_customer_id
,user_properties_user_id
,batch_page_id

,regexp_extract(page_location, r'^[^?]*')  as page_location
,split(page_location, '?')[SAFE_OFFSET(1)] as url_parameters
,page_referrer

,device_category
,device_mobile_brand_name

,geo_country 

,traffic_source_source


,manual_source 
,manual_content 
,manual_campaign_name 
from tmp_unnest

 {% if is_incremental() %}

    -- recalculate latest day's data + previous
    -- NOTE: The _dbt_max_partition variable is used to introspect the destination table
    where date(event_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)

{% endif %}
--where user_properties_user_id <> cast(user_id as int)

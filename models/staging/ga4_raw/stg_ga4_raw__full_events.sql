with 

source as (

    select * from {{ source('ga4_raw', 'full_events') }}

),

rownumbered as (

   SELECT row_number() over (order by event_timestamp) as rn, 
                event_date,   
                event_name,
                event_params

    from source

),

unrecorded as (
    select        rn,
        case when p.key = 'page_referrer' then p.value.string_value end as page_referrer,
          case when p.key = 'page_title' then p.value.string_value end as page_title
    from rownumbered, unnest(event_params) p
)

select rn, min(page_referrer) as page_referrer, min(page_title)  as page_title
from unrecorded
group by rn

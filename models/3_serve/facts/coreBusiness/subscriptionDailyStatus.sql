with subscriptionDailyArchive as 
(SELECT
  dbd ,f.* 
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2024-08-20', current_date(), INTERVAL 1 DAY)) as dbd
  join {{ ref('stg_snapshot__subscription_snapshot_check') }} f
  on cast(dbd as timestamp) between dbt_valid_from  and ifnull(dbt_valid_to  , CURRENT_TIMESTAMP())
)

select dbd, status , count(*) as cnt
from subscriptionDailyArchive
group by dbd, status

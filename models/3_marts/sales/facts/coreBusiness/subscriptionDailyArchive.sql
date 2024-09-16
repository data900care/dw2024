SELECT
  dateSnapshot ,subscriptionId, createdAt, updatedAt,status
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2024-08-20', current_date(), INTERVAL 1 DAY)) as dateSnapshot
  join {{ ref('stg_snapshot__subscription_snapshot_check') }} f
  on cast(dateSnapshot as timestamp) between dbt_valid_from  and ifnull(dbt_valid_to  , CURRENT_TIMESTAMP())


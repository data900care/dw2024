SELECT
  dateSnapshot ,subscriptionId, status,
  orderIntervalUnit,
  orderIntervalFrequency,
  price,
  sku,
  c.shopify_customerId,
  a.country
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2024-08-20', current_date(), INTERVAL 1 DAY)) as dateSnapshot
  join {{ ref('stg_snapshot__subscription_snapshot_check') }} f
  on cast(dateSnapshot as timestamp) between dbt_valid_from  and ifnull(dbt_valid_to  , CURRENT_TIMESTAMP())
  left join {{ ref('inner_shopify__customer') }} c 
    on c.recharge_latestCustomerid = f.rechargeCustomerId

  left join {{ ref('stg_shopify__customer_address') }} a 
    on a.shopify_customerId = c.shopify_customerId

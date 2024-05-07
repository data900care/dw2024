select 'grapevine' as dateSource,max(createdAt) as lastUpdate from {{ ref('stg_externalData__externalData_Grapevine_Survey_Data') }}
union all
select 'OKR' as dateSource, max(weekstart+6) as lastUpdate from {{ ref('stg_externalData__recharge_okr_CumSubscriptionsWeekly') }} 
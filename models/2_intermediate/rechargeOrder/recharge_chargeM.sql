with charge_summaryS  as 
(select id,min(cast(updated_at as date)) as firstTreatedAt, max(updated_at) as lastUpdatedAt
from {{ ref('stg_recharge__charges') }}
group by id),


endofFirstDay as 
(
select rc.id, max(updated_at) as lastUpdatedAt
from  charge_summaryS
join {{ ref('stg_recharge__charges') }} rc
on rc.id = charge_summaryS.id and  cast(rc.updated_at as date) = charge_summaryS.firstTreatedAt 
group by id
),


endofFirstDayCharge as 
(
select rc.id, status , rc.updated_at, error_type
from {{ ref('stg_recharge__charges') }}   rc
join endofFirstDay
on rc.id = endofFirstDay.id and rc.updated_at = endofFirstDay.lastUpdatedAt
),


 charge_summaryM as 
(
select charge_summaryS.id, firstTreatedAt , endofFirstDayCharge.status as endofFirstDayStatus,
endofFirstDayStatusError.error_type as firstErrorType, 
cast(charge_summaryS.lastUpdatedAt as date) as lastUpdatedAt , lastCharge.status as lastStatus
 from charge_summaryS

join {{ ref('stg_recharge__charges') }}  lastCharge
on lastCharge.id = charge_summaryS.id and lastCharge.updated_at = charge_summaryS.lastUpdatedAt

join endofFirstDayCharge 
on endofFirstDayCharge.id = charge_summaryS.id

left join endofFirstDayCharge endofFirstDayStatusError
on endofFirstDayStatusError.id = charge_summaryS.id and endofFirstDayStatusError.status = 'error'

)


select * from charge_summaryM 
--where errorDate is not null
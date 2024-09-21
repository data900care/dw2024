with charge_summaryS  as 
(select id,min(cast(scheduledAt as date)) as firstScheduledAt, max(updatedAt) as lastUpdatedAt
from {{ ref('stg_recharge__charges') }}
group by id),


endofFirstScheduleDay as 
(
select rc.id, max(updatedAt) as lastUpdatedAt
from  charge_summaryS
join {{ ref('stg_recharge__charges') }} rc
on rc.id = charge_summaryS.id and  cast(rc.scheduledAt as date) = charge_summaryS.firstScheduledAt 
group by id
),


endofFirstScheduleDayCharge as 
(
select rc.id, status , rc.updatedAt, errorType
from {{ ref('stg_recharge__charges') }}   rc
join endofFirstScheduleDay
on rc.id = endofFirstScheduleDay.id and rc.updatedAt = endofFirstScheduleDay.lastUpdatedAt
),


 charge_summaryM as 
(
select charge_summaryS.id, firstScheduledAt , endofFirstScheduleDayCharge.status as endofFirstScheduleDayStatus,
endofFirstDayStatusError.errorType as firstErrorType, 
cast(charge_summaryS.lastUpdatedAt as date) as lastUpdatedAt , 
lastCharge.status as lastStatus , 
case when lastCharge.status = 'error' then lastCharge.errorType else null end as  lastErrorType
 from charge_summaryS

join {{ ref('stg_recharge__charges') }}  lastCharge
on lastCharge.id = charge_summaryS.id and lastCharge.updatedAt = charge_summaryS.lastUpdatedAt

join endofFirstScheduleDayCharge 
on endofFirstScheduleDayCharge.id = charge_summaryS.id

left join endofFirstScheduleDayCharge endofFirstDayStatusError
on endofFirstDayStatusError.id = charge_summaryS.id and endofFirstDayStatusError.status = 'error'

)


select * from charge_summaryM 
--where errorDate is not null
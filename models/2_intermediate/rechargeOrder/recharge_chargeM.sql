with c  as 
(select id, min(cast(created_at as date)) as createdAt, max(updated_at) as lastUpdatedAt
from {{ ref('stg_recharge__charges') }}
where created_at > '2024-08-07'
group by id),

firstError as
(select id, min(updated_at) as firstErrorUpdatedAt
from {{ ref('stg_recharge__charges') }}
where status = 'error'
group by id),

rechargeStatus as 
(select id, updated_at as updated_at, status
from {{ ref('stg_recharge__charges') }}
),

 ch as 
(
select c.id,createdAt , cast(firstErrorUpdatedAt as date) as errorDate, 
et.error_type as firstErrorType, 
ls.status as lastStatus
 from c

join rechargeStatus ls
on ls.id = c.id and ls.updated_at = c.lastUpdatedAt

left join firstError
on firstError.id = c.id

left join {{ ref('stg_recharge__charges') }} et
on et.id = c.id and et.updated_at = firstErrorUpdatedAt
)


select * from ch 
--where errorDate is not null
select dateSnapshot, status , count(*) as cnt
from {{ ref('subscriptionDailyArchive') }}
group by dateSnapshot, status

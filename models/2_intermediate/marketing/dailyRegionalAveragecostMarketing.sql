with dailyRegionalSpend as
(select 
region,
spendDate,
cast(sum(cost) as int) as cost

from {{ ref('stg_externalData__sparkDo__900CARE__PLATFORM_DAILY_SPEND') }} s
join {{ ref('stg_BIContent900__content900_Campaign_Location') }} cl
on s.campaignLocation = cl.spark_campaign_location
group by region,spendDate
),
dailyRegionalNewClient as
(
select region, createdAt, count(*)  as newCustomerCount
from {{ ref('shopifyOrderL') }} o
where orderCustomerType = 'New'
group by region,createdAt

)
select c.region , s.spendDate, cost,newCustomerCount, cost/newCustomerCount as averageDailyNewClientCost
  from dailyRegionalNewClient c
left join dailyRegionalSpend s 
    on c.createdAt = s.spendDate
    and c.region = s.region


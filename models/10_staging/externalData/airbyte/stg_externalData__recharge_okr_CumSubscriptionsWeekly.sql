with 

source as (

    select * from {{ source('externalDataAirbyte', 'recharge_okr_CumSubscriptionsWeekly') }}

)

    select
        --cast(weekno as int) as weekNo,
        PARSE_DATE('%d/%m/%Y',  weekstart) as weekStart,
        --PARSE_DATE('%d/%m/%Y',  weekend) as weekEnd,
        --weekyear as weekYear,
      
        cast(cumsubscriptions as int) as cumulativeSubscriptions,
        cast(netsubscriptions as int) as netSubscriptions,
        cast(newSubscriptions as int) as newSubscriptions,
        cast(churnedSubscriptions as int) as churnedSubscriptions

    from source
    where weekstart is  not null



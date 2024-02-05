with 

source as (

    select * from {{ source('externalDataAirbyte', 'recharge_okr_CumSubscriptionsWeekly') }}

),

renamed as (

    select
        weekno,
        weekend,
        weekyear,
        weekstart,
        cumsubscriptions,
        netsubscriptions,
        newsubscriptions,
        churnedsubscriptions

    from source

)

select * from renamed

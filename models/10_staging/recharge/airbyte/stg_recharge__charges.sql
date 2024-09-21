with 

source as (

    select * from {{ source('recharge_airbyte', 'recharge2_charges') }}
    --where id in (select id from {{ source('recharge_airbyte', 'recharge2_charges') }} where status = 'queued')
    where  status in ('error', 'success') 
    and updated_at > '2024-08-08'

),

renamed as (

    select

        id,

        status,

        --merged_at as mergedAt,

        created_at as createdAt,
        error_type as errorType,

        updated_at as updatedAt,

        --orders_count ,
        --processed_at,
        scheduled_at as scheduledAt

    from source

)

select * from renamed

with 

source as (

    select * from {{ source('recharge_airbyte', 'recharge2_charges') }}


),

renamed as (

    select

        id,

        status,

        created_at as createdAt,
        error_type as errorType,

        updated_at as updatedAt,

        cast(scheduled_at as timestamp) as scheduledAt

    from source

)

select * from renamed
    where  updatedAt > '2024-08-08'
    --and status in ('error', 'success') 
    
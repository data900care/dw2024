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

        merged_at,

        created_at,
        error_type,

        updated_at,

        orders_count,
        processed_at,
        scheduled_at

    from source

)

select * from renamed

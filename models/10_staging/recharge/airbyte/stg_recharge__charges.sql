with 

source as (

    select * from {{ source('recharge_airbyte', 'recharge_charges') }}
    where id in (select id from {{ source('recharge_airbyte', 'recharge_charges') }} where status = 'queued')

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

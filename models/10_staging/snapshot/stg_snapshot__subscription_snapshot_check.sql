with 

source as (

    select * from {{ source('snapshot', 'subscription_snapshot_check') }}

),

renamed as (

    select
        
        id,
        created_at,
        updated_at,
        status,
        dbt_valid_from,
        dbt_valid_to

    from source

)

select * from renamed

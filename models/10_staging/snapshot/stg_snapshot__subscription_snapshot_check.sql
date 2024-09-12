with 

source as (

    select * from {{ source('snapshot', 'subscription_snapshot_check') }}

),

renamed as (

    select
        
        id as subscriptionId,
        created_at as createdAt,
        updated_at as updatedAt,
        status,
        dbt_valid_from,
        dbt_valid_to

    from source

)

select * from renamed

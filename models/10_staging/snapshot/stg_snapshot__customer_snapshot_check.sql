with 

source as (

    select * from {{ source('snapshot', 'customer_snapshot_check') }}

),

renamed as (

    select
        id as shopify_customerId,
        created_at as createdAt,
        updated_at as updatedAt,
        upper(state) as state,
        dbt_valid_from,
        dbt_valid_to

    from source

)

select * from renamed
--where state in ('ENABLED')

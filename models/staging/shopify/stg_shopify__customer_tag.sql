with 

source as (

    select * from {{ source('shopify', 'customer_tag') }}

),

renamed as (

    select
        customer_id as shopify_customerId,
        index,
        _fivetran_synced as fivetranSynced,
        value

    from source

)

select * from renamed

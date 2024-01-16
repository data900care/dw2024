with 

source as (

    select * from {{ source('recharge', 'order') }}

),

renamed as (

    select id as recharge_orderId,
    cast(external_order_id_ecommerce as int) as shopify_orderId,
    is_deleted as isDeleted,
    created_at as createdAt,
    scheduled_at as scheduledAt,
    shipped_date as shippedAt

    from source
)

select * from renamed
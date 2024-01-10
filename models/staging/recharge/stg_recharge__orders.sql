with 

source as (

    select * from {{ source('recharge', 'order') }}

),

renamed as (

    select id as recharge_orderId,
    cast(external_order_id_ecommerce as int) as external_order_id_ecommerce

    from source
)

select * from renamed
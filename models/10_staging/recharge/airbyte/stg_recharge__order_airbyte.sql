with 

source as (

    select * from {{ source('recharge_airbyte', 'recharge_orders') }}

),

renamed as (

    select id as recharge_orderId,
    cast(JSON_VALUE(external_order_id,'$.ecommerce')  as int)  as shopify_orderId,
    cast(JSON_VALUE(customer,'$.id')  as int) as recharge_customerId,
    --is_deleted as isDeleted,
    cast(datetime(created_at, "Europe/Paris") as date) as createdAt,
    round(total_price,2) as totalPrice
    --scheduled_at as scheduledAt,
    --shipped_date as shippedAt

    from source
)
,
duplicatesRemoved as (

    select 
    shopify_orderId,
    min(recharge_orderId) as recharge_orderId
    from renamed
    where shopify_orderId is not null
    group by shopify_orderId

)
select * from renamed
where recharge_orderId in (select recharge_orderId from  duplicatesRemoved)
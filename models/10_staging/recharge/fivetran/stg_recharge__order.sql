with 

source as (

    select * from {{ source('recharge', 'order') }}

),

renamed as (

    select id as recharge_orderId,
    cast(external_order_id_ecommerce as int) as shopify_orderId,
    customer_id as recharge_customerId,
    is_deleted as isDeleted,
    cast(datetime(created_at, "Europe/Paris") as date) as createdAt,
    round(cast(total_price as numeric),2) as totalPrice
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
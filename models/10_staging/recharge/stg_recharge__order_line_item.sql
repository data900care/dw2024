with 

source as (

    select * from {{ source('recharge', 'order_line_item') }}

),

renamed as (

    select
        order_id as recharge_orderId,
        purchase_item_id as subscriptionId,
        quantity,
        sku,
        total_price,
        unit_price,
        unit_price_includes_tax,
        index as lineIndex
    from source
    where purchase_item_type = 'subscription'
)

select * from renamed

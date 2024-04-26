with 

source as (

    select * from {{ source('recharge', 'order_line_item') }}

)

    select
        order_id as recharge_orderId,
        purchase_item_id as subscriptionId,
        quantity,
        sku,
        round(cast(total_price as numeric),2) as totalPrice,
        round(cast(unit_price as numeric),2) as unitPrice,
        --unit_price_includes_tax,
        index as lineIndex
    from source
    where purchase_item_type = 'subscription'


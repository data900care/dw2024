with 

source as (

    select * from {{ source('recharge', 'order_line_item') }}

),

renamed as (

    select
        index,
        order_id as recharge_orderId,
        purchase_item_id as subscriptionId,
        quantity,
        sku,
        tax_due,
        taxable,
        taxable_amount,
        title,
        total_price,
        unit_price,
        unit_price_includes_tax,
        index as lineIndex
    from source

)

select * from renamed

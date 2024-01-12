with 

source as (

    select * from {{ source('shopify', 'order') }}

),

renamed as (

    select
        id as shopify_orderId,
        billing_address_country,
        cancel_reason,
        cancelled_at,
        confirmed,
        created_at,
        currency,
        current_subtotal_price,
        current_subtotal_price_set,
        current_total_discounts,
        current_total_discounts_set,
        current_total_duties_set,
        current_total_price,
        current_total_price_set,
        current_total_tax,
        current_total_tax_set,
        customer_id as shopify_customerId,
        name as orderName,
        number,
        order_number,
        processed_at,
        shipping_address_country,
        subtotal_price,
        subtotal_price_set,
        taxes_included,
        test,
        total_discounts,
        total_discounts_set,
        total_line_items_price,
        total_line_items_price_set,
        total_price,
        total_price_set,
        total_shipping_price_set,
        total_tax,
        total_tax_set,
        updated_at

    from source

)

select * from renamed

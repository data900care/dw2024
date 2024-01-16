with 

source as (

    select * from {{ source('shopify', 'order') }}

),

renamed as (

    select
        id as shopify_orderId,
        billing_address_country,
        cancel_reason as cancelReason,
        cancelled_at as cancelledAt,
        confirmed,
        created_at as createdAt,
        currency,
        current_subtotal_price,
        current_total_discounts,
        current_total_price,
        current_total_tax,
        customer_id as shopify_customerId,
        name as orderName,
        number,
        order_number,
        processed_at,
        shipping_address_country,
        subtotal_price,
        taxes_included,
        test,
        total_discounts,
        total_line_items_price,
        total_price,
        total_tax

    from source

)

select * from renamed

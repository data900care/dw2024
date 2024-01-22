with 

source as (

    select * from {{ source('shopify', 'order') }}

),

renamed as (

    select
        id as shopify_orderId,
        billing_address_country as billCountry,
        cancel_reason as cancelReason,
        cast(datetime(cancelled_at, "Europe/Paris") as date) as cancelledAt,
        case when cancelled_at is null then false else true end as cancelled,
        confirmed,
        cast(datetime(created_at, "Europe/Paris") as date) as createdAt,
        currency,
        current_subtotal_price,
        current_total_discounts,
        current_total_price,
        current_total_tax,
        customer_id as shopify_customerId,
        name as orderName,
        number,
        order_number,
        shipping_address_country as shippingCountry,
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

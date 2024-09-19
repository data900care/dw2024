with 

source as (

    select * from {{ source('shopify', 'order') }}

)

    select
        id as shopify_orderId,
        billing_address_country as billCountry,
        cancel_reason as cancelReason,
        cast(datetime(cancelled_at, "Europe/Paris") as date) as cancelledAt,
        case when cancelled_at is null then false else true end as cancelled,
        confirmed,
        cast(datetime(created_at, "Europe/Paris") as date) as createdAt,
        currency,
        customer_id as shopify_customerId,
        name as orderName,
        
        order_number as shopify_orderNumber,
        shipping_address_country as shippingCountry,

        test,
        fulfillment_status as orderFulfillmentStatus,
        round(total_line_items_price,2) as subTotal,
        round(total_discounts,2) as discount,
        round(total_tax) as tax,
        taxes_included as taxesIncluded,
        round(total_price,2) as total -- = total_line_items_price - total_discounts + total_shipping + total_tax (if taxes_included is false)

    from source s



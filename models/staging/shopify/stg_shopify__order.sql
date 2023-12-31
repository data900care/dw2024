with 

source as (

    select * from {{ source('shopify', 'order') }}

),

renamed as (

    select
        id,
        _fivetran_deleted,
        _fivetran_synced,
        app_id,
        billing_address_address_1,
        billing_address_address_2,
        billing_address_city,
        billing_address_company,
        billing_address_country,
        billing_address_country_code,
        billing_address_first_name,
        billing_address_last_name,
        billing_address_latitude,
        billing_address_longitude,
        billing_address_name,
        billing_address_phone,
        billing_address_province,
        billing_address_province_code,
        billing_address_zip,
        browser_ip,
        buyer_accepts_marketing,
        cancel_reason,
        cancelled_at,
        cart_token,
        checkout_id,
        checkout_token,
        client_details_user_agent,
        closed_at,
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
        customer_id,
        customer_locale,
        device_id,
        email,
        financial_status,
        fulfillment_status,
        landing_site_base_url,
        landing_site_ref,
        location_id,
        name,
        note,
        note_attributes,
        number,
        order_number,
        order_status_url,
        original_total_duties_set,
        payment_gateway_names,
        presentment_currency,
        processed_at,
        reference,
        referring_site,
        shipping_address_address_1,
        shipping_address_address_2,
        shipping_address_city,
        shipping_address_company,
        shipping_address_country,
        shipping_address_country_code,
        shipping_address_first_name,
        shipping_address_last_name,
        shipping_address_latitude,
        shipping_address_longitude,
        shipping_address_name,
        shipping_address_phone,
        shipping_address_province,
        shipping_address_province_code,
        shipping_address_zip,
        source_identifier,
        source_name,
        source_url,
        subtotal_price,
        subtotal_price_set,
        taxes_included,
        test,
        token,
        total_discounts,
        total_discounts_set,
        total_line_items_price,
        total_line_items_price_set,
        total_price,
        total_price_set,
        total_shipping_price_set,
        total_tax,
        total_tax_set,
        updated_at,
        user_id

    from source

)

select * from renamed

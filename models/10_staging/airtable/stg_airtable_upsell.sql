with 

source as (

    select * from {{ source('airtable', 'airtable_airupsell___basefullupsells_tblRQj1DtQsrDcWef') }}

)

    select
    
        cast(date_upsell as date) as dateUpsell,
        upsell_type as upsellType,
        contact_channel as contactChannel,
        upsellid as upsellId,
        upsell_first_order_original_order as originalOrderName,
        cast(upsell_subscription_id_recharge as int) as subscriptionId,
        upsell_first_order_new_order as newOrderName

    from source



with 

source as (

    select * from {{ source('airtable', 'airtable_airupsell___recurring_customers_tbleQksIw2sqeQorX') }}

),

renamed as (

    select
        
        statut,
        date_upsell as dateUpsell,
        upsell_type as upsellType,
        cast(subscription_id as int) as subscriptionId

    from source

)

select * from renamed

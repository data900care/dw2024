with 

source as (

    select * from {{ source('externalDataAirbyte', 'externalData_Grapevine_Survey_Data') }}

)

        select 
        cast(order_id as int) as shopify_orderId,
        cast(left(created_at,10) as date) as createdAt, --just for data refreshness stats
        answer 

    from source




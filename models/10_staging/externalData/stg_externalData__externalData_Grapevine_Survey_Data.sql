with 

source as (

    select * from {{ source('externalDataAirbyte', 'externalData_Grapevine_Survey_Data') }}

),

renamed as (

        select
        cast(order_id as int) as shopify_orderId,
        cast(left(created_at,10) as date) as createdAt,
        answer 

    from source

)

select * from renamed


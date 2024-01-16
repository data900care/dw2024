with 

source as (

    select * from {{ source('externalData', 'externalData_Grapevine_Survey_Data') }}

),

renamed as (

        select
        cast(order_id as int) as shopify_orderId,
        answer 

    from source

)

select * from renamed


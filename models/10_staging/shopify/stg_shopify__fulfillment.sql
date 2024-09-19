with 

source as (

    select * from {{ source('shopify', 'fulfillment') }}

),

dedup as (

    select order_id as shopify_orderId,
        status 
     FROM source
        where id in
            (
            SELECT max(id) as id FROM source
            group by order_id

            )

)

select * from dedup

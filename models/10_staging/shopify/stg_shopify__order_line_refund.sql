with 

source as (

    select * from {{ source('shopify', 'order_line_refund') }}

),

renamed as (

    select

        order_line_id,
        refund_id,
        subtotal

    from source

)

select * from renamed

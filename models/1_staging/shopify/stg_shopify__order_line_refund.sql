with 

source as (

    select * from {{ source('shopify', 'order_line_refund') }}

),

renamed as (

    select
        
        quantity,
        refund_id as refundId,        
        subtotal,        
        total_tax  as totalTax      

    from source

)

select * from renamed

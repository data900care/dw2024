with 

source as (


    select
        id as recharge_orderId,
        cast(JSON_VALUE(l,'$.purchase_item_id') as int) as subscriptionId,
        JSON_VALUE(l,'$.purchase_item_type')  as purchase_item_type,
        cast(JSON_VALUE(l,'$.quantity') as int) as quantity,
        JSON_VALUE(l,'$.sku')  as sku,
        round(cast(JSON_VALUE(l,'$.total_price') as numeric),2) as totalPrice,
        round(cast(JSON_VALUE(l,'$.unit_price') as numeric),2) as unitPrice,
        --unit_price_includes_tax,
        -1 + ROW_NUMBER() over (PARTITION BY id) as lineIndex
    from {{ source('recharge_airbyte', 'recharge_orders') }}
    left join   unnest( json_extract_array(line_items) ) as l
    
)
select * from source
where purchase_item_type = 'subscription'



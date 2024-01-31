

with 

source as (

    select * from {{ source('shopify', 'order_note_attribute') }}

),


urlDecoded as (



    SELECT order_id as shopify_orderId,
                SAFE_CONVERT_BYTES_TO_STRING(
            ARRAY_TO_STRING(ARRAY_AGG(
                IF(STARTS_WITH(y, '%'), FROM_HEX(SUBSTR(y, 2)), CAST(y AS BYTES)) ORDER BY i
            ), b'')) as data
        FROM  source , UNNEST(REGEXP_EXTRACT_ALL(value, r"%[0-9a-fA-F]{2}|[^%]+")) AS y WITH OFFSET AS i 
    where name = 'utm_campaign' 
    group by order_id
),
cleaned as (select shopify_orderId, replace(data,'"','') as data from urlDecoded)



select * from cleaned

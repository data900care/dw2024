

with 

source as (

    select * from {{ ref('stg_shopify__order_note_attribute') }}
    where name in ('utm_content' ,'utmContent' )
),


urlDecoded as (



    SELECT  shopify_orderId,
                SAFE_CONVERT_BYTES_TO_STRING(
            ARRAY_TO_STRING(ARRAY_AGG(
                IF(STARTS_WITH(y, '%'), FROM_HEX(SUBSTR(y, 2)), CAST(y AS BYTES)) ORDER BY i
            ), b'')) as data
        FROM  source , UNNEST(REGEXP_EXTRACT_ALL(value, r"%[0-9a-fA-F]{2}|[^%]+")) AS y WITH OFFSET AS i 
    group by shopify_orderId
),
cleaned as (select shopify_orderId, replace(data,'"','') as data from urlDecoded)



select * from cleaned

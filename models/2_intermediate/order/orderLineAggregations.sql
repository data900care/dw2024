with aggregations as 
(select
    shopify_orderid,
    count(*) as itemCount,
    sum(price) as totalItemPrice,
    sum(quantity) as totalItemQuantity,
    count(distinct sku) as distinctSkuCount
from {{ ref("stg_shopify__order_line") }}

group by  shopify_orderId)

select * from aggregations

select
    shopify_orderid,
    count(*) as itemcount,
    round(sum(price),2) as totalitemprice,
    sum(quantity) as totalitemquantity,
    count(distinct sku) as distinctskucount
from {{ ref("stg_shopify__order_line") }}

group by shopify_orderid

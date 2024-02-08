select shopify_customerId, sku, productType , productCategory, quantity

from {{ ref("inner_shopify__order") }} o
join {{ ref("inner_shopify__order_line") }} ol using (shopify_orderid)
join {{ ref("stg_BIContent900__content900_Product") }} p using (sku)

where customerorderrank = 1

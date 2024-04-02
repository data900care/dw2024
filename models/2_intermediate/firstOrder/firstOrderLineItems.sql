select shopify_customerId, sku, productType , productCategory, quantity

from {{ ref("inner_shopify__order") }} o
join {{ ref("inner_shopify__order_line") }} ol using (shopify_orderid)

where customerorderrank = 1

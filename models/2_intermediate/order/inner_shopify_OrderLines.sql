select sol.*, so.validorder

from {{ ref("stg_shopify__order_line") }} sol
left join {{ ref("shopifyOrders") }} so using (shopify_orderid)

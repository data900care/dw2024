innerselect sol.*, so.validorder

from {{ ref("stg_shopify__order_line") }} sol
left join {{ ref("inner_shopify_order") }} so using (shopify_orderid)

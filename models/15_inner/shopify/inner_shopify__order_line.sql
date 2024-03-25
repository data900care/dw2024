select sol.*, so.validorder, p.productsPerBasket , pc.impact_on_recurring_order_basket_size, p.productCategory,productType
from {{ ref("stg_shopify__order_line") }} sol
left join {{ ref("inner_shopify__order") }} so using (shopify_orderid)
left join {{ ref('stg_BIContent900__content900_Product') }} p using(sku)
join {{ ref('stg_BIContent900__content900_Product_Category') }} pc on p.productCategory = pc.category

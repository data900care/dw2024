select sol.*, 
p.basketSizeQuantity , p.recurring_order_quantity_divide, p.productCategory,productType
from {{ ref("stg_shopify__order_line") }} sol

left join {{ ref('stg_BIContent900__content900_Product') }} p using(sku)


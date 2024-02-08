select shopify_orderId,     
        price,
        
        quantity,
        sku,
        totalDiscount
        
from {{ ref('inner_shopify__order_line') }}
select shopify_orderId,     
        price,
        
        quantity,
        sku,
        totalDiscount
        
from {{ ref('stg_shopify__order_line') }}
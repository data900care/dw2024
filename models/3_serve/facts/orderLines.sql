select shopify_orderId,     
        price,
        productId,
        quantity,
        sku,
        totalDiscount,
        variantId
from {{ ref('stg_shopify__order_line') }}
select distinct shopify_orderId, true as orderWithTrialKit
    FROM {{ ref('stg_shopify__order_line') }}
    where SKU in 
(select sku from {{ ref('stg_BIContent900__content900_Product') }} 
    where isTrialKit )
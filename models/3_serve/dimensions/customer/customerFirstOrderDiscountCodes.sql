select * from {{ ref('orderDiscountCodes') }}
where shopify_orderId in ( select shopify_orderId from {{ ref('customerFirstOrder') }})
select shopify_orderId, createdAt, amount
 from {{ ref("stg_shopify__transaction") }} t
where kind = 'refund'
select *
 from {{ ref("stg_shopify__transaction") }} t
where kind = 'refund'
with
onlyProductReviews as (
    select createdAt,
    shopify_orderId,
    sku,
    productTitle,
    score
    from {{ ref('stg_externalData__yotpo') }}
    where type = 'product_review'
)

select * from onlyProductReviews
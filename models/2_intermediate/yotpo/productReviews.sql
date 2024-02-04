with
onlyProductReviews as (
    select createdAt,
    shopify_orderId,
    sku,
    score
    from {{ ref('stg_externalData2__yotpo') }}
    where type = 'product_review'
)

select * from onlyProductReviews

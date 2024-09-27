select createdAt,
    countryName,
    sku,
    productTitle,
    score,
    reviewerEmail,
    shopify_orderId,
    'yotpo' as source 
    from {{ ref('yotpoReviewsM') }}
    where type = 'product_review'
union all
select createdAt,
    countryName,
    sku,
    productName,
    rating,
    reviewerEmail,
    shopify_orderId,
    'reviewsIO' as source
    from {{ ref('reviewsioReviewM') }}


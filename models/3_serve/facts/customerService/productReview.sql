select createdAt,
    countryName,
    sku,
    productTitle,
    score,
    reviewerEmail,
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
    'reviewsIO' as source
    from {{ ref('reviewsioReviewM') }}


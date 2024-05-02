select createdAt,
    countryName,
    sku,
    productTitle,
    score,
    'yotpo' as source 
    from {{ ref('yotpoReviewsM') }}
    where type = 'product_review'
union all
select createdAt,
    countryName,
    sku,
    productName,
    rating,
    'reviewsIO' as source
    from {{ ref('reviewsioReviewM') }}


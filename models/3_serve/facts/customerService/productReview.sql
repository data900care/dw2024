select createdAt,
    countryName,
    sku,
    productTitle,
    score
    from {{ ref('reviewsM') }}
    where type = 'product_review'
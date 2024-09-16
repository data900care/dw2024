with extendedReviews as (
    select r.createdAt,
    shippingCountry,
    sku,
    productTitle,
    c.countryName as reviewerCountryName,
    score,
    r.type,
    avis900,
    reviewerEmail
    from {{ ref('stg_externalData__yotpo') }} r
        left join {{ ref('stg_shopify__order') }} o
            using (shopify_orderNumber)
        left join {{ ref('stg_BIContent900__content900_Country') }} c
            on   c.yotpoReviewerCountryCode = r.reviewerCountry)

select createdAt,
    IFNULL(shippingCountry,reviewerCountryName) as countryName,
    sku,
    productTitle,
    score,
    avis900,
    reviewerEmail,
    type
from extendedReviews
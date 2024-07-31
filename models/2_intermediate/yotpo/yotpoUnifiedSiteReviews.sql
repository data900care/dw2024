with
avis900Reviews as (
    select 
    distinct 
    createdAt,
    countryName,
    avis900 as score,
    reviewerEmail
    from {{ ref('yotpoReviewsM') }}
    where type = 'product_review'
    and avis900 > 0
),
siteReviews as (
    select 
    distinct
    createdAt,
    countryName,
    score*2 as score,
    reviewerEmail
    from {{ ref('yotpoReviewsM') }}
    where type = 'site_review'
)

select  *  from siteReviews
union all
select * from avis900Reviews
with
avis900Reviews as (
    select createdAt,
    shopify_orderId,
    avis900 as score
    from {{ ref('stg_externalData__yotpo') }}
    where type = 'product_review'
    and avis900 > 0
),
siteReviews as (
    select createdAt,
    shopify_orderId,
    score*2 as score
    from {{ ref('stg_externalData__yotpo') }}
    where type = 'site_review'
)

select * from siteReviews
union all
select * from avis900Reviews
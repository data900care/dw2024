select 
    createdAt,
    score, 
    countryName,
    customerType,
    customerStatus,
    'retently' as source
    from {{ ref('feedbackM') }}

union all

select 
    createdAt,
    score, 
    countryName,
    null,null,
    'yotpo' as source
    from {{ ref('unifiedSiteReviews') }}
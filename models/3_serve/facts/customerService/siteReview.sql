select 
    createdAt,
    score, 
    countryName,
    customerType,
    customerStatus,
    reviewerEmail,
    'retently' as source
    from {{ ref('retentlyFeedbackM') }}

union all

select 
    createdAt,
    score, 
    countryName,
    null,null,
    reviewerEmail,
    'yotpo' as source
    from {{ ref('yotpoUnifiedSiteReviews') }}
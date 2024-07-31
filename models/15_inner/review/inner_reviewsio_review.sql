select distinct b.reviewerEmail ,a.* 
from {{ ref('stg_reviewsio__review_and_question') }} a 
left join {{ ref('stg_reviewsio__company_review_invitation') }} b
using (shopify_orderNumber)

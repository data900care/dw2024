select createdAt, sku, reviewerEmail, f.* 
from {{ ref('stg_reviewsio__review_attribute') }} f
join {{ ref('inner_reviewsio_review') }} 
using(review_and_question_id)
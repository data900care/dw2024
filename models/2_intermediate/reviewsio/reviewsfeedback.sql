select createdAt, sku, f.* from {{ ref('stg_reviewsio__review_attribute') }} f
join {{ ref('stg_reviewsio__review_and_question') }} 
using(review_and_question_id)
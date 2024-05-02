with 

source as (

    select * from {{ source('reviewsio', 'review_and_question') }}

)

    select
        id as review_and_question_id,
        cast(date_created as date) as createdAt, 
        order_id  as orderName,
        rating,
        sku,
        product_name as productName

    from source
    where type = 'product_review'



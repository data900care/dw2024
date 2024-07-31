with 

source as (

    select * from {{ source('reviewsio', 'review_and_question') }}

)

    select
        id as review_and_question_id,
        cast(date_created as date) as createdAt, 
        safe_cast(replace(order_id,'#','') as int)  as shopify_orderNumber,
        rating,
        sku,
        product_name as productName

    from source
    where type = 'product_review'



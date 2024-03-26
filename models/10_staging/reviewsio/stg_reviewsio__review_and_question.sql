with 

source as (

    select * from {{ source('reviewsio', 'review_and_question') }}

),

renamed as (

    select
        author_location,
        date_created, 
        order_id as shopify_orderNo,
        rating,
        sku,
        source,
        type,
        type_label

    from source

)

select * from renamed

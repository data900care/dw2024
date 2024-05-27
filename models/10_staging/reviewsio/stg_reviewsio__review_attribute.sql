with

source as (

    select * from {{ source('reviewsio', 'review_attribute') }}

),

renamed as (

    select
        review_and_question_id,
        label,
        value as answers
    from source

)

 

    select  review_and_question_id,label,substr(answer,2, LENGTH(answer) - 2) as answer
    from renamed,
     UNNEST(JSON_QUERY_ARRAY(answers,'$')) as answer

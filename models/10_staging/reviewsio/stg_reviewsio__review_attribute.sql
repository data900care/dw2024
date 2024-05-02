with

source as (

    select * from {{ source('reviewsio', 'review_attribute') }}

),

renamed as (

    select
        review_and_question_id,
        label,
        --value as answers
        normalize(replace(REPLACE(value, '\\u00e9', 'è'),'\\u00e0','à')) as answers

    from source

)



   

    select  review_and_question_id,label,answer 
    from renamed,
     UNNEST(SPLIT(SUBSTR(answers, 2, LENGTH(answers) - 2))) as answer

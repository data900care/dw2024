with 

source as (

    select * from {{ source('retently', 'retently_feedback') }}

)

    select   
        cast (score as int) as score,  
        country, 
        cast(left(createddate,10) as date) as createdAt, 
        JSON_EXTRACT_SCALAR(feedbackTagsNew, '$[0]') as customerType,
        JSON_EXTRACT_SCALAR(feedbackTagsNew, '$[1]') as customerStatus
    from source



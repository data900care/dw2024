with 

source as (

    select * from {{ source('reviewsio', 'company_review_invitation') }}

),

renamed as (

    select
        
        customer_email as reviewerEmail,
        
        safe_cast(replace(order_id,'#','') as int)  as shopify_orderNumber
      

    from source

)

select shopify_orderNumber, min(reviewerEmail) as reviewerEmail
from renamed
where REGEXP_CONTAINS(reviewerEmail, r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,6}$') is true
group by shopify_orderNumber

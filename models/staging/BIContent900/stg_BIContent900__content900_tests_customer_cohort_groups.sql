with 

source as (

    select * from {{ source('BIContent900', 'content900_tests_customer_cohort_groups') }}

),

renamed as (

    select
        email,
        cohort

    from source

)

select * from renamed

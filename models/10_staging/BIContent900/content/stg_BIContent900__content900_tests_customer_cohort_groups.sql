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
,
duplicates as (
    select
        email
    from source
    group by email 
    having count(distinct cohort) > 1

)
select distinct  email, cohort from renamed
where email not in (select email from duplicates)
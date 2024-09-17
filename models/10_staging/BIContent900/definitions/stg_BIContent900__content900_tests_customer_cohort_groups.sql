with 

source as (

    select * from {{ source('BIContent900', 'content900_tests_customer_cohort_groups') }}

)
,
duplicates as (
    select
        email
    from source
    group by email 
    having count(distinct cohort) > 1

)
select distinct  email, cohort from source
where email not in (select email from duplicates)
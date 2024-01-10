with 

source as (

    select * from {{ source('externalData', 'externalData_Grapevine_Survey_Data_csv') }}

),

renamed as (

    select
        order_id,
        how_heard

    from source
    where how_heard is not null 
    union all
        select
        order_id,
        answer as how_heard

    from source
    where how_heard is  null 
)

select * from renamed


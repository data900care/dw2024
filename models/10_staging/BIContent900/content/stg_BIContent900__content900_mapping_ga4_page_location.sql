with 

source as (

    select * from {{ source('BIContent900', 'content900_mapping_ga4_page_location') }}

),

renamed as (

    select
        page_location,
        new_page_location

    from source

)

select * from renamed

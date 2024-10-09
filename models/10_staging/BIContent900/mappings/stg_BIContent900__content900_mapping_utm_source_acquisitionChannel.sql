with 

source as (

    select * from {{ source('BIContent900', 'content900_mapping_utm_source_acquisitionChannel') }}

),

renamed as (

    select
        utm_source,
        acquisitionchannel

    from source

)

select * from renamed

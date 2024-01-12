with 

source as (

    select * from {{ source('BIContent900', 'content900_mapping_Grapevine_acquisitionChannel') }}

),

renamed as (

    select
      
        howheard,
        acquisitionChannel as acquisitionChannel

    from source

)

select * from renamed

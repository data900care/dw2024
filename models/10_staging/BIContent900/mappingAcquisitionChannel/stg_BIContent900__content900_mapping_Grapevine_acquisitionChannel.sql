with 

source as (

    select * from {{ source('BIContent900', 'content900_mapping_Grapevine_acquisitionChannel') }}

)

    select
      
        howheard,
        acquisitionChannel as acquisitionChannel

    from source



with 

source as (

    select * from {{ source('BIContent900', 'content900_Campaign_Location') }}

),

renamed as (

    select
       
        region,
        spark_campaign_location

    from source

)

select * from renamed

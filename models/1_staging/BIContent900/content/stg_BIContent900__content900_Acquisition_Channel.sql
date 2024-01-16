with 

source as (

    select * from {{ source('BIContent900', 'content900_Acquisition_Channel') }}

),

renamed as (

    select
        acquisition_channel as acquisitionChannel,
        acquisition_channel_type as acquisitionChannelType

    from source

)

select * from renamed

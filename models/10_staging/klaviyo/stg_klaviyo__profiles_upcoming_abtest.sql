with 

source as (

    select * from {{ source('klaviyo', 'klaviyo_profiles') }}

),

renamed as (

    select

        JSON_VALUE(attributes,'$.email') as email,
        JSON_VALUE(attributes,'$.properties.upcoming_abtest') as upcoming_abtest

    from source
        
)

select * from renamed
where upcoming_abtest is not null
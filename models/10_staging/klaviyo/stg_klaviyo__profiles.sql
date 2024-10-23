with 

source as (

    select * from {{ source('klaviyo', 'klaviyo_profiles') }}

),

renamed as (

    select

        JSON_VALUE(attributes,'$.email') as email,
        JSON_VALUE(attributes,'$.properties.upcoming_abtest') as upcoming_abtest_value
        /*JSON_VALUE(attributes,'$.properties.newProperty') as newProperty_value*/
    from source
        
)

select email,'upcoming_abtest' as property, upcoming_abtest_value as propertyValue 
from renamed
where upcoming_abtest_value is not null
/*
select email,'newProperty' as property, upcoming_abtest_value as newPropertyValue 
from renamed
where newPropertyValue is not null
*/
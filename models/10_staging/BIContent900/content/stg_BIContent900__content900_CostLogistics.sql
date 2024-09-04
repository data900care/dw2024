with 

source as (

    select * from {{ source('BIContent900', 'content900_CostLogistics') }}

),

renamed as (

    select
     
       
        year,
        month,
        region,
        split(clienttype,",") as clienttypes,
        cost

    from source

)

select renamed.* except(clienttypes), clientType from renamed
CROSS JOIN UNNEST(renamed.clienttypes) AS clientType
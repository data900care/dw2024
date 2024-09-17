with 

source as (

    select * from {{ source('BIContent900', 'content900_CostLogistics') }}

),

renamed as (

    select
     
       
        cast(year as int) as year,
        cast(month as int) as month,
        region,
        split(clienttype,",") as clienttypes,
        cast(replace(cost,',','.') as decimal) as cost

    from source

)

select renamed.* except(clienttypes), clientType from renamed
CROSS JOIN UNNEST(renamed.clienttypes) AS clientType
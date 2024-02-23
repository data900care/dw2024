with 

source as (

    select * from {{ source('BIContent900', 'content900_Product_Category') }}

),

renamed as (

    select
      
        category,
        cast(display_order as int) as display_order,
        impact_on_recurring_order_basket_size

    from source

)

select * from renamed

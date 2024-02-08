with 

source as (

    select * from {{ ref('stg_shopify__transaction') }}
    where kind = 'refund'
)


select *
from source
 
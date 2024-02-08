select * from {{ ref('extended_shopifyOrder') }}
where validOrder = true
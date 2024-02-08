select * from {{ ref('shopifyOrders') }}
where validOrder = true
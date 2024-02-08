with ordersBefore20220117 as
(
    select shopify_orderId,customerOrderRank from {{ ref('validShopifyOrders') }} where createdAt < '2022-01-17'
),

ordersWithRecurringTag as 
(
  select shopify_orderId from    {{ ref('stg_shopify__order_tag') }}  where CONTAINS_SUBSTR(tag , 'Recurring')
)


select shopify_orderId, 'New' as customerType
from ordersBefore20220117 so
where customerOrderRank = 1 

union all


select shopify_orderId, 'Recurring' as customerType
from ordersBefore20220117 so
where customerOrderRank > 1 and 
exists (select 1 from   ordersWithRecurringTag t   where   t.shopify_orderId = so.shopify_orderId)

union all

select shopify_orderId, 'Existing' as customerType
from ordersBefore20220117 so
where customerOrderRank > 1 and
not exists (select 1 from   ordersWithRecurringTag t   where   t.shopify_orderId = so.shopify_orderId)


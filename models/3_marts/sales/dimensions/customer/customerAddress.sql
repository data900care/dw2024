select
    sc.shopify_customerId,
    sc.createdAt ,
    o.createdAt as firstOrderDate,
    totalSpent,
    sc.recharge_latestCustomerid as recharge_latestCustomerid,
    sc.email,
        address_1,
        address_2,
        city,
        country,
        first_name,
        zip

from {{ ref("inner_shopify__customer") }} sc
join  {{ ref("inner_shopify__order") }} o on o.shopify_customerId = sc.shopify_customerId and  customerorderrank = 1
left join {{ ref('stg_shopify__customer_address') }} a on a.shopify_customerId = sc.shopify_customerId

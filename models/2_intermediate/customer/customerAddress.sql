select
    sc.shopify_customerId,
    sc.recharge_latestCustomerid as recharge_latestCustomerid,
    sc.email,
        address_1,
        address_2,
        city,
        country,
        first_name,
        zip

from {{ ref("inner_shopify__customer") }} sc
left join {{ ref('stg_shopify__customer_address') }} a on a.customer_id = shopify_customerId
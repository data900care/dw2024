with
    unifiedorderlines as (
        select rol.*, sol.validorder
        from {{ ref("inner_recharge_orderLine") }} rol
        left join
            {{ ref("inner_shopify_orderLine") }} sol
            on rol.shopify_orderid = sol.shopify_orderid
            and sol.lineIndex = rol.lineIndex + 1
    )

select *
from unifiedorderlines

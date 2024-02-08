with
    unifiedorderlines as (
        select rol.*, sol.validorder
        from {{ ref("inner_recharge_orderline") }} rol
        left join
            {{ ref("inner_shopify_orderline") }} sol
            on ro.shopify_orderid = sol.shopify_orderid
            and sol.lineIndex = rol.lineIndex + 1
    )
select *
from unifiedorderlinesinn

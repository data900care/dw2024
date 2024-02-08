with
    unifiedrechargeorderlines as (
        select rol.*, sol.validorder
        from {{ ref("stg_recharge__order_line_item") }} rol
        join {{ ref("stg_recharge__orders") }} ro using (recharge_orderid)
        left join
            {{ ref("inner_shopify_OrderLines") }} sol
            on ro.shopify_orderid = sol.shopify_orderid
            and sol.lineIndex = rol.lineIndex + 1
    )
select *
from unifiedrechargeorderlines

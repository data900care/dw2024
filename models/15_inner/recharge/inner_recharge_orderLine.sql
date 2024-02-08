with
    innerjoin as (
        select rol.*, ro.shopify_orderId
        from {{ ref("stg_recharge__order_line_item") }} rol
        join {{ ref("stg_recharge__order") }} ro using (recharge_orderid)
    )

select *
from innerjoin

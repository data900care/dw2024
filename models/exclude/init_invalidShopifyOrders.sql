with
    notcanceledorders as (
        select * from {{ ref("stg_shopify__order") }} where cancelled is false
    ),
    fullInvalid as (
        (
            select shopify_orderId, invalidLabel
            from notcanceledorders
            join
                {{ ref("stg_invalidOrder_customerIds") }} i
                on shopify_customerid = i.customerid
        )
        union all
        (
            select shopify_orderId, invalidLabel
            from {{ ref("stg_shopify__order_discount_code") }} od
            join notcanceledorders using (shopify_orderid)
            join
                {{ ref("stg_invalidOrder_testDiscountCodes") }} i
                on od.discountcode = i.discountcode
                and matchtype = 'equal'
        )
        union all
        (
            select shopify_orderId, invalidLabel
            from {{ ref("stg_shopify__order_discount_code") }} od
            join notcanceledorders using (shopify_orderid)
            join
                {{ ref("stg_invalidOrder_testDiscountCodes") }} i
                on od.discountcode like concat(i.discountcode, '%')
                and matchtype = 'starts'
        )
        union all
        (
            select shopify_orderId, invalidLabel
            from {{ ref("stg_shopify__order_discount_code") }} od
            join notcanceledorders using (shopify_orderid)
            join
                {{ ref("stg_invalidOrder_testDiscountCodes") }} i
                on lower(od.discountcode) like concat('%', lower(i.discountcode), '%')
                and matchtype = 'includes'  -- CONTAINS_SUBSTR ?
        )
        union all
        (
            select shopify_orderId, invalidLabel
            from notcanceledorders so
            join
                {{ ref("stg_invalidOrder_orderNames") }} i on so.ordername = i.ordername
        )
        union all
        (
            select shopify_orderId, 'Initial order before upsell' as invalidLabel
            from {{ ref("stg_shopify__order_tag") }}
            join notcanceledorders using (shopify_orderid)
            where
                lower(tag) in (
                    'upsellcancel',
                    'upsellcncel',
                    'upsellcancell',
                    'upcellcancel',
                    'cancelupsell'
                )
        )
        union all
        (
            select shopify_orderId, 'No Recharge Order Tags'
            from notcanceledorders so

            where
                not exists (
                    select 1
                    from {{ ref("stg_shopify__order_tag") }} t
                    where
                        regexp_contains(tag, 'Subscription|OTP')
                        and t.shopify_orderid = so.shopify_orderid
                )
        )
    )
select shopify_orderId, min(invalidLabel) as invalidLabel
from fullInvalid
group by shopify_orderid

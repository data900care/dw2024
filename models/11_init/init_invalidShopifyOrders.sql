/*(
select shopify_orderId, concat('Canceled: ' , cancelReason) as invalidLabel
from {{ ref('stg_shopify__order') }} 
where cancelledAt is not null
)
union all
*/
with
    fullinvalid as (
        (
            select shopify_orderid, invalidlabel
            from {{ ref("stg_shopify__order") }}
            join
                {{ ref("stg_invalidOrder_customerIds") }} i
                on shopify_customerid = i.customerid
        )
        union all
        (
            select shopify_orderid, invalidlabel
            from {{ ref("stg_shopify__order_discount_code") }} od
            join
                {{ ref("stg_invalidOrder_testDiscountCodes") }} i
                on od.discountcode = i.discountcode
                and matchtype = 'equal'
        )
        union all
        (
            select shopify_orderid, invalidlabel
            from {{ ref("stg_shopify__order_discount_code") }} od
            join
                {{ ref("stg_invalidOrder_testDiscountCodes") }} i
                on od.discountcode like concat(i.discountcode, '%')
                and matchtype = 'starts'
        )
        union all
        (
            select shopify_orderid, invalidlabel
            from {{ ref("stg_shopify__order_discount_code") }} od
            join
                {{ ref("stg_invalidOrder_testDiscountCodes") }} i
                on lower(od.discountcode) like concat('%', lower(i.discountcode), '%')
                and matchtype = 'includes'  -- CONTAINS_SUBSTR ?
        )
        union all
        (
            select shopify_orderid, invalidlabel
            from {{ ref("stg_shopify__order") }} so
            join
                {{ ref("stg_invalidOrder_orderNames") }} i on so.ordername = i.ordername
        )
        union all
        (
            select shopify_orderid, 'Initial order before upsell' as invalidlabel
            from {{ ref("stg_shopify__order_tag") }}
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
            select shopify_orderid, 'No Recharge Order Tags'
            from {{ ref("stg_shopify__order") }} so
            -- left join {{ ref("stg_shopify__order_discount_code") }} d using
            -- (shopify_orderid)
            where
                not exists (
                    select 1
                    from {{ ref("stg_shopify__order_tag") }} t
                    where
                        regexp_contains(tag, 'Subscription|OTP')
                        and t.shopify_orderid = so.shopify_orderid
                )
        -- and d.discountCode <> '900BASSADEUR'
        )
    )
select shopify_orderid, min(invalidlabel)
from fullinvalid
group by shopify_orderid

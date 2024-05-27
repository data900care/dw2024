with
orderstoInvalidate as 
    (select shopify_orderId,shopify_customerid,orderName,cancelled ,cancelReason 
    from {{ ref("stg_shopify__order")}} ),


invalidCustomerOrders as
    (
            select shopify_orderId, invalidLabel
            from orderstoInvalidate
            join
                {{ ref("stg_invalidOrder_customerIds") }} i
                on shopify_customerid = i.customerid
        ),
invalidOrderNameOrders as 
   (
            select shopify_orderId, invalidLabel
            from orderstoInvalidate so
            join
                {{ ref("stg_invalidOrder_orderNames") }} i on so.ordername = i.ordername
            where shopify_orderId not in (select shopify_orderId  from invalidCustomerOrders)
        ),
withoutRechargeOrderTagOrders as 
        (
            select shopify_orderId, 'No Recharge Order Tags'
            from orderstoInvalidate so
            where
                not exists (
                    select 1
                    from {{ ref("stg_shopify__order_tag") }} t
                    where
                        regexp_contains(tag, 'Subscription|OTP')
                        and t.shopify_orderid = so.shopify_orderid
                )
            and shopify_orderId not in (select shopify_orderId  from invalidCustomerOrders)
            and shopify_orderId not in (select shopify_orderId  from invalidOrderNameOrders)
        ),
invalidDiscountCodeOrders as
(


           ( select shopify_orderId, invalidLabel
            from {{ ref("stg_shopify__order_discount_code") }} od
            join orderstoInvalidate using (shopify_orderid)
            join
                {{ ref("stg_invalidOrder_testDiscountCodes") }} i
                on od.discountcode = i.discountcode
                and matchtype = 'equal')
             union all
            (
            select shopify_orderId, invalidLabel
            from {{ ref("stg_shopify__order_discount_code") }} od
            join orderstoInvalidate using (shopify_orderid)
            join
                {{ ref("stg_invalidOrder_testDiscountCodes") }} i
                on od.discountcode like concat(i.discountcode, '%')
                and matchtype = 'starts'
            )
            union all
            (
                select shopify_orderId, invalidLabel
                from {{ ref("stg_shopify__order_discount_code") }} od
                join orderstoInvalidate using (shopify_orderid)
                join
                    {{ ref("stg_invalidOrder_testDiscountCodes") }} i
                    on lower(od.discountcode) like concat('%', lower(i.discountcode), '%')
                    and matchtype = 'includes'  -- CONTAINS_SUBSTR ?
            )
        ),
invalidDiscountCodeOrdersDistinct as
        ( select shopify_orderId, max(invalidLabel) as invalidLabel
        from invalidDiscountCodeOrders
        where shopify_orderId not in (select shopify_orderId  from invalidCustomerOrders)
            and shopify_orderId not in (select shopify_orderId  from invalidOrderNameOrders)
            and shopify_orderId not in (select shopify_orderId  from withoutRechargeOrderTagOrders) 
        group by shopify_orderId
        ),
all900InvalidatedOrders as 
        (select * from invalidCustomerOrders
        union all
        select * from invalidOrderNameOrders
        union all
        select * from withoutRechargeOrderTagOrders
        union all
        select * from invalidDiscountCodeOrdersDistinct),

 forUpsellCancelledOrders as 
    (select  shopify_orderId ,  max(concat('upsell:' , upsellType)) as invalidLabel 
    from orderstoInvalidate
    join {{ ref('stg_airtable_upsell') }} on  orderName = originalOrderName
    where cancelled is true 
         and shopify_orderId not in (select shopify_orderId  from all900InvalidatedOrders)
    group by shopify_orderId
    ),
canceledOrderswithoutRefund as 
    (select shopify_orderId, concat('cancel: ' , cancelReason) as invalidLabel
    from orderstoInvalidate 
    where cancelled is true 
            and shopify_orderId not in (select shopify_orderId  from {{ ref('inner_shopify_refund_transaction') }})
            and shopify_orderId not in (select shopify_orderId  from all900InvalidatedOrders)
            and shopify_orderId not in (select shopify_orderId  from forUpsellCancelledOrders)
    )

select * from all900InvalidatedOrders
union all
select * from forUpsellCancelledOrders
union all
select * from canceledOrderswithoutRefund



with
orderstoInvalidate as 
    (select shopify_orderId,shopify_customerid,orderName,cancelled ,cancelReason 
    from {{ ref("stg_shopify__order")}} ),



cancelledOrdersForUpsell as 
    (select  shopify_orderId ,  max(concat('upsell:' , upsellType)) as invalidLabel 
    from orderstoInvalidate
    join {{ ref('stg_airtable_upsell') }} on  orderName = originalOrderName
    where cancelled is true 
    group by shopify_orderId
    ),
cancelledOrdersForGrouping as
(
    
            select distinct shopify_orderId, 'MERGER_OLD' as invalidLabel
            from {{ ref("stg_shopify__order_tag") }}
            join orderstoInvalidate using (shopify_orderid)
            where
                lower(tag) in (
                    'merger_old'
                )
            and shopify_orderId not in (select shopify_orderId  from cancelledOrdersForUpsell)
        
),
canceledOrderswithoutRefund as 
    (select shopify_orderId, concat('cancel/norefund: ' , cancelReason) as invalidLabel
    from orderstoInvalidate 
    where cancelled is true 
            and shopify_orderId not in (select shopify_orderId  from {{ ref('inner_shopify_refund_transaction') }})

            and shopify_orderId not in (select shopify_orderId  from cancelledOrdersForUpsell)
            and shopify_orderId not in (select shopify_orderId  from cancelledOrdersForGrouping)
    )


select * from cancelledOrdersForUpsell
union all
select * from cancelledOrdersForGrouping
union all
select * from canceledOrderswithoutRefund

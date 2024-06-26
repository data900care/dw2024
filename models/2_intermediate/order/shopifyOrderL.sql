with oct as 
    (select
        o.*,
        case
            when totalRefund = 0 then 'NoRefund'
            when totalRefund between total and total+0.01 then 'FullRefund'
            when totalRefund < total then 'PartialRefund'
            when totalRefund > total+0.01 then 'ExtraRefund'
        end     
        as refundStatus,
        coalesce(c2020.customertype, c2023.customertype) as orderCustomerType

    from {{ ref("shopifyOrderM") }} o

    left join {{ ref("orderCustomerType2020") }} c2020 using (shopify_orderid)
    left join {{ ref("orderCustomerType2022") }} c2023 using (shopify_orderid))
,
cotr as 
    (
    select *,
    case when orderCustomerType is null then null
    else 
    rank() over (
            partition by shopify_customerid,orderCustomerType order by shopify_orderId
        ) 
    end 
    as customerOrderTypeRank
    from oct)

 select *,
 case when orderCustomerType = 'Recurring'
    then
        case when customerOrderTypeRank = 1 
        then  'Recurring 1'
        else 
         'Recurring 2+'
         end
    else
    orderCustomerType
end as newOrderCustomerType

  from cotr



with orderLines as 
(select shopify_orderId,   
        lineIndex,
        price,
        quantity,
        case orderCustomerType
            when 'Recurring'
                then 
                if(quantity / recurring_order_quantity_divide < 1 , 1, round(quantity / recurring_order_quantity_divide))
                else
                quantity
        end as quantity_adjusted,
        basketSizeQuantity,
        sku,
        totalDiscount,
        orderCustomerType,
        productCategory,
        productType,
        createdAt
        
from {{ ref('inner_shopify__order_line') }}
join {{ ref('shopifyOrderL') }} using (shopify_orderId)
)

select shopify_orderId,     
        lineIndex,
        price,
        
        quantity,
        quantity_adjusted,
        sku,
        totalDiscount,
      
        quantity_adjusted * basketSizeQuantity as basketSum,
        cost as costProduction
from orderLines ol
left join {{ ref('costProduction') }} cp
    on cp.year = extract(year from ol.createdAt) 
        and cp.month = extract(month from ol.createdAt) 
        and cp.productCategory = ol.productCategory
        and cp.productType = ol.productType

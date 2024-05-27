with orderLines as 
(select shopify_orderId,     
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
        orderCustomerType
        
from {{ ref('inner_shopify__order_line') }}
join {{ ref('shopifyOrderL') }} using (shopify_orderId)
)

select shopify_orderId,     
        price,
        
        quantity,
        quantity_adjusted,
        sku,
        totalDiscount,
      
        quantity_adjusted * basketSizeQuantity as basketSum
from orderLines

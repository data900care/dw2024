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
        --quantity * basketSizeQuantity as basketSum,
        --recurring_order_quantity_divide,
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
        /*
        case orderCustomerType
            when 'Recurring'
                then 
                    case when basketSum / recurring_order_quantity_divide < 1 
                    then 1
                    else
                    basketSum / recurring_order_quantity_divide
                    end
            else
               basketSum
        end as basketSum*/
       
        quantity_adjusted * basketSizeQuantity as basketSum
from orderLines

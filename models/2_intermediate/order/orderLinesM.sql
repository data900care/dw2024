with orderLines as 
(select shopify_orderId,     
        price,
        
        quantity,
        sku,
        totalDiscount,
        quantity * basketSizeQuantity as basketSum,
        impact_on_recurring_order_basket_size,
        orderCustomerType
        
from {{ ref('inner_shopify__order_line') }}
join {{ ref('shopifyOrderL') }} using (shopify_orderId)
)

select shopify_orderId,     
        price,
        
        quantity,
        sku,
        totalDiscount,
        
        case orderCustomerType
            when 'Recurring'
                then 
                    case when basketSum * impact_on_recurring_order_basket_size <1 
                    then 1
                    else
                    basketSum * impact_on_recurring_order_basket_size
                    end
            else
               basketSum
        end as basketSum
from orderLines

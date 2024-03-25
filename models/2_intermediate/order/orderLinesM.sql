with orderLines as 
(select shopify_orderId,     
        price,
        
        quantity,
        sku,
        totalDiscount,
        quantity * productsPerBasket as basketSum,
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
            when 'recurring'
                then basketSum * impact_on_recurring_order_basket_size
            else
               basketSum
        end as basketSum
from orderLines

select * from {{ ref('shopifyOrderL') }} o

where validorder = false
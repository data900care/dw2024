select * from {{ ref('shopifyOrderL') }}
where validorder = true
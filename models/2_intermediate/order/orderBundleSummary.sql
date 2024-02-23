select shopify_orderId , count(*) as bundleCount 
from 
{{ ref('orderBundles_from_order_note_attribute') }}
group by shopify_orderId
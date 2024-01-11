select shopify_orderid, acquisitionChannel
from {{ ref("order_acquisition_channel_01_via_grapevine") }}
union all
select shopify_orderid, acquisitionChannel
from {{ ref("order_acquisition_channel_02_via_discountCode") }}

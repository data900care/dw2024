select shopify_orderid, acquisitionChannel
from {{ ref("orderAcquisitionChannel_01_via_grapevine") }}
union all
select shopify_orderid, acquisitionChannel
from {{ ref("orderAcquisitionChannel_02_via_discountCode") }}

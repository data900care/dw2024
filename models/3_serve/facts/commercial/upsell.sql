select dateUpsell,
        upsellType,
        contactChannel
from {{ ref('upsellM') }}
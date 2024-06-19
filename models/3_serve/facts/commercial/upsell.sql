select dateUpsell,
        upsellType,
        contactChannel,
        reduction
from {{ ref('upsellM') }}
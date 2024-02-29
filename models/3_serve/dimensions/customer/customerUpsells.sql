select shopify_customerId , upsellType
from {{ ref('subscriptionM') }}
where upsellType is not null
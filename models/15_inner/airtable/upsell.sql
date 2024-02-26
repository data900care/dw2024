select subscriptionid, upsellType
from {{ ref("stg_airtable_upsell") }}


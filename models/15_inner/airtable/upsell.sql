select subscriptionid, 'new' as upsellcustomertype
from {{ ref("stg_airtable_upsell_new_customers") }}
union all
select subscriptionid, 'recurring' as upsellcustomertype
from {{ ref("stg_airtable_upsell_recurring_customers") }}

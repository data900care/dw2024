select * except(idAdresse) from {{ ref('subscriptionM') }}

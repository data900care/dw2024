select
    c.shopify_customerid,
    s.*,
    rank() over (
        partition by shopify_customerid order by subscriptionId
    ) as customersubscriptionrank,
    a.countrycode,
    cy.countryName,
    case orderIntervalUnit 
        when 'day' then orderIntervalFrequency
        when 'week' then orderIntervalFrequency * 7
        when 'month' then orderIntervalFrequency * 30
    end as orderIntervalFrequencyDay
from {{ ref("init_invalidHardDeleteSubscriptions") }} s 
join
    {{ ref("stg_recharge__customer_airbyte") }} c 
    using(recharge_customerid)
left join {{ ref("stg_recharge__address_airbyte") }} a  
    on a.id = s.idadresse
left join {{ ref('stg_BIContent900__content900_Country') }} cy 
    on cy.recharge_countryCode = a.countrycode

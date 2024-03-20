select
    c.shopify_customerid,
    s.*,
    rank() over (
        partition by shopify_customerid order by subscriptionId
    ) as customersubscriptionrank,
    a.countrycode,
    cy.countryName,
    case orderIntervalUnit 
    when 'week' then orderIntervalFrequency * 7
    when 'day' then orderIntervalFrequency
    when 'month' then orderIntervalFrequency * 30
    end as orderIntervalFrequencyDay
from {{ ref("stg_recharge__subscription") }} s
join
    {{ ref("stg_recharge__customer") }} c
    on s.recharge_customerid = c.recharge_customerid
left join {{ ref("stg_recharge__address") }} a on a.id = s.idadresse
left join {{ ref('stg_BIContent900__content900_Country') }} cy 
    on cy.recharge_countryCode = a.countrycode

select region, clientType, year, month, max(cost) as cost
from {{ ref('stg_BIContent900__content900_CostLogistics') }}
group by region, clientType, year, month
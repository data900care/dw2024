select productType,productCategory, year, month, max(cost) as cost
from {{ ref('stg_BIContent900__content900_CostProduction') }}
group by productType,productCategory, year, month
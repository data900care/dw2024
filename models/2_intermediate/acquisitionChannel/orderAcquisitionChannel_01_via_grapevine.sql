select  o.shopify_orderId, min(g.acquisitionChannel) as acquisitionChannel
from {{ ref("stg_shopify__orders") }} o
join
    {{ ref("stg_externalData__externalData_Grapevine_Survey_Data") }} s using (shopify_orderId)
join
    {{ ref("stg_BIContent900__content900_mapping_Grapevine_acquisitionChannel") }} g
    on regexp_contains(lower(s.answer), lower(g.howheard))
group by o.shopify_orderId

--quand nos règles match plusieurs Acquisition channel :
--Si on peut prendre la première réponse donnée c'est bien, sinon au hasard no pbm . Camille
--11/01/2024

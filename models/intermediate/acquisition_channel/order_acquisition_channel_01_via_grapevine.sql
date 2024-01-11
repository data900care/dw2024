select  o.shopify_orderid, min(g.acquisitionchannel) as acquisitionchannel
from {{ ref("unified_orders") }} o
join
    {{ ref("stg_externalData__externalData_Grapevine_Survey_Data") }} s
    on o.shopify_orderid = s.shopify_orderid
join
    {{ ref("stg_BIContent900__content900_mapping_Grapevine_acquisitionChannel") }} g
    on regexp_contains(lower(s.answer), lower(g.howheard))
group by o.shopify_orderid

--quand nos règles match plusieurs Acquisition channel :
--Si on peut prendre la première réponse donnée c'est bien, sinon au hasard no pbm . Camille
--11/01/2024

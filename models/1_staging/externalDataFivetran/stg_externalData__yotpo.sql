with

    source as (select * from {{ source("externalDataFivetran", "yotpo") }}),

    renamed as (

        select
            cast(review_creation_date as date) as createdAt,
            order_date as orderdate,
            order_id as shopify_orderId,
            product_sku as sku,
            product_title as productTitle,
            review_type as type,
            review_score as score,
            case
                when
                    contains_substr(
                        cf_default_form_votre_avis_sur_900_care,
                        '1 - Jamais de la vie !'
                    ) 
                then 1
                when
                    contains_substr(
                        cf_default_form_votre_avis_sur_900_care, '10 - Grave '
                    ) 
                then 10
                else  cast(cf_default_form_votre_avis_sur_900_care as int)
            end as avis900
        from source

    )

select *
from renamed

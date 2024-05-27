select  s.createdAt, 
        rating,
        sku,
        productName ,
        billCountry as countryName
        from {{ ref('stg_reviewsio__review_and_question') }} s
      left join {{ ref('inner_shopify__order') }} o
            using (orderName)
    
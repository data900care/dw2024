select  s.createdAt, 
        rating,
        sku,
        productName ,
        billCountry as countryName,
        reviewerEmail
        from {{ ref('inner_reviewsio_review') }} s
      left join {{ ref('inner_shopify__order') }} o
            using (shopify_orderNumber)
    
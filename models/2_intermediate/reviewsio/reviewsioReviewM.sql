select  s.createdAt, 
        s.rating,
        s.sku,
        s.productName ,
        o.shippingCountry as countryName,
        
        s.reviewerEmail,
        shopify_orderId
        from {{ ref('inner_reviewsio_review') }} s
      left join {{ ref('stg_shopify__order') }} o
            using (shopify_orderNumber)
    
with
    customerbundlesfromvalidorders as

    (
        select shopify_customerid, ok.shopify_orderid, bundletype
        from {{ ref("orderBundles_from_order_note_attribute") }} ok
        join {{ ref("inner_shopify__order") }} o using (shopify_orderid)
        where validorder = true
    )

select distinct shopify_customerid, bundletype
from customerbundlesfromvalidorders

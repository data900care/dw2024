SELECT upsell_first_order_original_order 
FROM {{ source('airtable', 'airtable_airupsell___basefullupsells_tblRQj1DtQsrDcWef') }}
where upsell_first_order_original_order is not null
group by upsell_first_order_original_order
having count(*) > 1
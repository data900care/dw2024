select s.*,
m.new_page_location
     from {{ ref('stg_ga4_events') }} s
left join {{ ref('stg_BIContent900__content900_mapping_ga4_page_location') }} m
    using (page_location)
with stateUpdateCatched as 
    (select id from {{ ref('stg_snapshot__customer_snapshot_check') }} 
    group by id 
    having count(distinct state) > 1)


SELECT id,date_diff(updatedAt,createdAt,day) as diff
FROM {{ ref('stg_snapshot__customer_snapshot_check') }}
join stateUpdateCatched using(id)
where  state = 'ENABLED'
qualify  RANK() OVER (PARTITION BY id ORDER BY updatedAt) = 1
with ranked as 
(
    select o.*,
    rank() over (
        partition by recharge_customerId order by recharge_orderId
    ) as customerOrderRank
     from {{ ref('stg_recharge__order') }} o
)
select * from ranked
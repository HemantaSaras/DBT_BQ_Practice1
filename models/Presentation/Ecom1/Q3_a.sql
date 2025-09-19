with ranked as (
    select
        o.customer_id,
        o.paid_amount as total_spend,
        row_number() over (order by o.paid_amount desc) as rn
    from {{ ref('fact_Ecom1_Orders') }} o
)
select
    customer_id,
    total_spend
from ranked
where rn <= 10
order by total_spend desc
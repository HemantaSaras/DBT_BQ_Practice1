with ranked as (
    select
        s.product_id,
        s.product_name,
        sum(s.quantity * s.product_price) as total_revenue,
        row_number() over (order by sum(s.quantity * s.product_price) desc) as revenue_rank
    from {{ ref("fact_Sales") }} s
    group by s.product_id, s.product_name
)

select *
from ranked
where revenue_rank <= 10

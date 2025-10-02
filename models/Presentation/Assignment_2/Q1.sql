with ranked as (
    select
        s.customer_id,
        s.customer_name,
        sum(s.quantity * s.product_price) as total_revenue,
        row_number() over (order by sum(s.quantity * s.product_price) desc) as revenue_rank
    from {{ ref("fact_Sales") }} s
    group by s.customer_id, s.customer_name
)

select *
from ranked
where revenue_rank <= 10

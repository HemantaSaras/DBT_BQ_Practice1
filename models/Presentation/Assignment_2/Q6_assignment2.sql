with ranked as (
    select
        s.product_category,
        sum(s.quantity * s.product_price) as total_revenue,
        row_number() over (order by sum(s.quantity * s.product_price) desc) as revenue_rank
    from {{ ref("fact_Sales") }} s
    group by s.product_category
)

select *
from ranked
where revenue_rank <= 10

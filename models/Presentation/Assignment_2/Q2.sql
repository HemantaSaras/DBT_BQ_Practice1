select
    format_date('%Y-%m', s.sale_date) as year_month,
    count(distinct s.customer_id) as unique_customers
from {{ ref("fact_Sales") }} s
group by year_month
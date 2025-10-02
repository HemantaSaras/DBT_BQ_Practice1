select 
    p.payment_method,
    sum(p.amount) as total_revenue
from {{ ref("fact_Payments")}} p
where p.payment_status = 'Success'
group by p.payment_method
order by total_revenue desc

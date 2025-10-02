select
    payment_method,
    avg(amount)
from {{ ref('fact_Payments') }}
group by payment_method
select
    payment_method,
    count(payment_id) as total_payments,
    avg(amount) as avg_amount
from {{ ref('fact_Payments') }}
group by payment_method

-- how many payments done in each payment_method 
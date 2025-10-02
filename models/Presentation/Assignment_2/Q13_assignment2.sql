select
    (count(case when payment_status in ('Pending', 'Failed') then 1 end) * 100.0 / count(*)) as pct_payments_failed_or_pending
from {{ ref('fact_Payments') }}

SELECT
    payment_id,
    sale_id,
    payment_method,
    payment_status,
    payment_date,
    amount
from
    {{ source("job2", "Payments")}}
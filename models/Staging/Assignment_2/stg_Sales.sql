select
    sale_id,
    customer_id,
    product_id,
    quantity,
    sale_date
from
    {{ source("job2", "Sales")}}
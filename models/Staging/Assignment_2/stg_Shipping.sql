select
    shipping_id,
    sale_id,
    shipping_date,
    delivery_date,
    shipping_cost,
    shipping_status
from
    {{ source("job2", "Shipping")}}
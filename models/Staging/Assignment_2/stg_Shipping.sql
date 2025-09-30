select
    shipping_id,
    sale_id,
    shipping_id,
    delivery_date,
    shipping_cost,
    shipping_status
from
    {{ source("job2", "Shipping")}}
select
    customer_location,
    avg(delivery_date - shipping_date) as avg_shipping_time
from {{ ref('fact_Shipping') }}
group by customer_location